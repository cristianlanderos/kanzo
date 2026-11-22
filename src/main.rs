mod auth;
mod export;
mod home;
mod mcp;
mod service;
mod share;
#[cfg(unix)]
mod stdio;
mod store;
mod sync;
mod update;
mod web;

use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Parser, Subcommand, ValueEnum};

use crate::home::Home;

/// Self-hosted kanban for humans and agents.
///
/// Run `kanzo` to open the board. Agents connect with `kanzo mcp`.
#[derive(Parser)]
#[command(name = "kanzo", version)]
struct Cli {
    /// Data folder [default: $KANZO_HOME, then ~/kanzo]
    #[arg(long, global = true, value_name = "DIR")]
    home: Option<PathBuf>,

    /// Port for the board [default: from config.toml]
    #[arg(long)]
    port: Option<u16>,

    /// Start the board without opening the browser
    #[arg(long)]
    no_open: bool,

    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand)]
enum Command {
    /// Run the MCP server on stdio (agents launch this, not you)
    Mcp,
    /// Create the data folder; the first run does this too
    Init {
        /// Folder to create [default: the data folder]
        folder: Option<PathBuf>,
    },
    /// Commit the data folder and pull or push it with its git remote
    Sync {
        /// Sync with this git repo from now on, like https://github.com/you/kanzo-data or you/kanzo-data
        #[arg(long, value_name = "URL")]
        repo: Option<String>,
        /// When both sides changed, keep this machine's board and overwrite the remote
        #[arg(long, conflicts_with = "keep_remote")]
        keep_local: bool,
        /// When both sides changed, take the remote board and drop this machine's changes
        #[arg(long)]
        keep_remote: bool,
    },
    /// Write the whole board as Markdown or JSON
    Export {
        /// Output format
        #[arg(long, short, value_enum, default_value = "markdown")]
        format: export::Format,
        /// Only this project
        #[arg(long, short)]
        project: Option<String>,
        /// Write to this file instead of the terminal
        #[arg(long, short, value_name = "FILE")]
        output: Option<PathBuf>,
    },
    /// Share the board with other computers on your network, or stop; without on or off, show whether it is
    Share {
        #[arg(value_enum)]
        state: Option<OnOff>,
    },
    /// Use the board another computer shares, from this computer's board and agents
    Join {
        /// The address `kanzo share on` printed there, like 192.168.0.12
        host: String,
    },
    /// Stop using the shared board, and go back to this computer's own
    Leave,
    /// Install the latest release over this Kanzo, and restart the board's service
    Update,
    /// Start the board automatically at login
    Service {
        #[command(subcommand)]
        action: ServiceAction,
    },
    /// Manage the board's accounts from this computer, like to reset a forgotten password
    User {
        #[command(subcommand)]
        action: UserAction,
    },
}

#[derive(Subcommand)]
enum UserAction {
    /// List the users
    List,
    /// Add a user, with the password read from the terminal; the first one turns logins on and is an admin
    Add {
        name: String,
        /// Let them add and remove users
        #[arg(long)]
        admin: bool,
    },
    /// Set a user's password, read from the terminal, and sign them out everywhere
    Password { name: String },
    /// Remove a user; their tasks and comments stay. Removing the last one turns logins off
    Remove { name: String },
}

#[derive(Clone, Copy, ValueEnum)]
enum OnOff {
    On,
    Off,
}

#[derive(Subcommand)]
enum ServiceAction {
    /// Install and start a service that runs the board at login (LaunchAgent on macOS, systemd on Linux, Task Scheduler on Windows)
    Install,
    /// Stop and remove that service
    Uninstall,
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    if cfg!(windows)
        && let Ok(exe) = std::env::current_exe()
    {
        update::remove_old_exes(&exe);
    }
    // The board serves one person and each agent starts its own `kanzo mcp`, so one async
    // thread is enough, and blocking threads exit when idle: this keeps kanzo light on small boards.
    let outcome = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .max_blocking_threads(8)
        .build()
        .map_err(anyhow::Error::from)
        .and_then(|rt| rt.block_on(run(cli)));
    match outcome {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("kanzo: {e:#}");
            ExitCode::FAILURE
        }
    }
}

async fn run(cli: Cli) -> anyhow::Result<()> {
    // Before opening the data folder: it may be the reason to update.
    if let Some(Command::Update) = cli.command {
        return update_kanzo();
    }
    let dir = match &cli.command {
        Some(Command::Init { folder: Some(folder) }) => folder.clone(),
        _ => Home::resolve(cli.home)?,
    };
    let mut home = Home::open(dir)?;
    if let Some(port) = cli.port {
        home.config.port = port;
    }
    match cli.command {
        None => web::serve(home, !cli.no_open).await,
        Some(Command::Mcp) => mcp::serve(home).await,
        Some(Command::Init { .. }) => {
            println!("Kanzo data folder ready at {}", home.dir.display());
            Ok(())
        }
        Some(Command::Sync { repo, keep_local, keep_remote }) => {
            if let Some(url) = repo {
                println!("The board syncs with {}.", sync::set_remote(&home, &url, true)?);
            }
            let keep = match (keep_local, keep_remote) {
                (true, _) => Some(sync::Keep::Local),
                (_, true) => Some(sync::Keep::Remote),
                _ => None,
            };
            println!("{}", sync::sync(&home, keep, true)?);
            Ok(())
        }
        Some(Command::Export { format, project, output }) => {
            export::export(&home, format, project.as_deref(), output.as_deref())
        }
        Some(Command::Share { state }) => {
            let change = state.map(|s| match s {
                OnOff::On => share::Change::On,
                OnOff::Off => share::Change::Off,
            });
            network(home, change, !cli.no_open).await
        }
        Some(Command::Join { host }) => network(home, Some(share::Change::Join(host)), !cli.no_open).await,
        Some(Command::Leave) => network(home, Some(share::Change::Leave), !cli.no_open).await,
        Some(Command::Service { action: ServiceAction::Install }) => service::install(&home),
        Some(Command::Service { action: ServiceAction::Uninstall }) => service::uninstall(),
        Some(Command::User { action }) => users(&home, action),
        Some(Command::Update) => unreachable!("handled above"),
    }
}

/// Whoever can run this already reads and writes the data folder, so it asks for no login.
fn users(home: &Home, action: UserAction) -> anyhow::Result<()> {
    let store = home.store()?;
    let password = || -> anyhow::Result<String> {
        eprint!("Password (shown as you type): ");
        let mut line = String::new();
        std::io::stdin().read_line(&mut line)?;
        let password = line.trim_end_matches(['\r', '\n']);
        auth::check_password(password).map_err(anyhow::Error::msg)?;
        Ok(auth::hash_password(password))
    };
    match action {
        UserAction::List => {
            let users = store.list_users()?;
            if users.is_empty() {
                println!("No users: the board asks for no login. Add the first with `kanzo user add <name>`.");
            }
            for user in users {
                println!("{}{}", user.name, if user.admin { " (admin)" } else { "" });
            }
        }
        UserAction::Add { name, admin } => {
            let first = !store.has_users()?;
            let user = store.create_user(&name, &password()?, admin)?;
            let role = if user.admin { " as an admin" } else { "" };
            println!("Added {}{role}.", user.name);
            if first {
                println!("The board now asks everyone to sign in.");
            }
        }
        UserAction::Password { name } => {
            let user = store.user_by_name(&name)?;
            let patch = store::UserPatch { password: Some(password()?), ..Default::default() };
            store.update_user(user.id, patch, None)?;
            println!("Changed the password of {}, and signed them out everywhere.", user.name);
        }
        UserAction::Remove { name } => {
            let user = store.delete_user(store.user_by_name(&name)?.id)?;
            for file in store.unused_images(user.avatar.into_iter().collect())? {
                let _ = std::fs::remove_file(home.images().join(file));
            }
            println!("Removed {}.", user.name);
            if !store.has_users()? {
                println!("No users are left: the board asks for no login.");
            }
        }
    }
    Ok(())
}

fn update_kanzo() -> anyhow::Result<()> {
    let Some(latest) = update::latest(true) else {
        anyhow::bail!("can't reach GitHub to check for updates");
    };
    if !update::newer(&latest) {
        println!("Kanzo is up to date ({}).", update::CURRENT);
        return Ok(());
    }
    println!("Installing Kanzo {latest} (you have {})…", update::CURRENT);
    update::install().map_err(anyhow::Error::msg)?;
    if service::restart()? {
        println!("Updated to {latest}, and restarted the board.");
    } else {
        println!("Updated to {latest}.");
    }
    Ok(())
}

async fn network(home: Home, change: Option<share::Change>, open: bool) -> anyhow::Result<()> {
    let sharing = matches!(change, Some(share::Change::On));
    let (status, running) = share::request(&home, change).await?;
    if sharing && !running {
        return web::serve(home, open).await;
    }
    println!("{}", status.message());
    Ok(())
}
