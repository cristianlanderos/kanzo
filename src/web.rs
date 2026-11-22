use std::collections::{HashMap, HashSet};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use axum::body::Bytes;
use axum::extract::{ConnectInfo, DefaultBodyLimit, Path, Query, Request, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::middleware::{self, Next};
use axum::response::{Html, IntoResponse, Response};
use axum::routing::{any, delete, get, patch, post};
use axum::{Extension, Json, Router};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

use crate::auth::{self, Who};
use crate::home::Home;
use crate::share::{self, Change, Net};
use crate::store::{self, Error, ImagesOf, Store, TaskPatch, User, UserPatch};
use crate::{sync, update};

const INDEX: &str = include_str!("index.html");
const IMAGE_LIMIT: usize = 25 * 1024 * 1024;
const APP_JS: &str = include_str!("app.js");
const ZH_CN_JS: &str = include_str!("zh-cn.js");
const LOGO: &[u8] = include_bytes!("logo.png");
const ICON: &[u8] = include_bytes!("icon.png");
const CHANGELOG: &str = include_str!("../CHANGELOG.md");
const MANIFEST: &str = r##"{"id":"/","name":"Kanzo","start_url":"/","display":"standalone","background_color":"#ffffff","icons":[{"src":"/icon.png","sizes":"192x192 512x512","type":"image/png"}]}"##;

/// Scripts only from this server, so even a bug in the Markdown renderer cannot run injected script.
/// The one inline script, which sets the theme before the first paint, runs by its hash.
const CSP: &str = "default-src 'none'; script-src 'self' 'sha256-qbmIwOLWfXjTJcvp/pTTPC8bx25itrEad7276b6Vh5A='; style-src 'self' 'unsafe-inline'; img-src 'self' data:; \
                   connect-src 'self'; manifest-src 'self'; base-uri 'none'; form-action 'none'; frame-ancestors 'none'";

pub async fn serve(home: Home, open: bool) -> Result<()> {
    let port = home.config.port;
    let guard = Arc::new(Guard::new(port, home.config.listen, Net::load(&home)).with_token(share::token(&home)));
    let url = format!("http://localhost:{port}");
    let listener = match TcpListener::bind(SocketAddr::new(guard.address(), port)).await {
        Ok(l) => l,
        Err(e) if e.kind() == std::io::ErrorKind::AddrInUse => {
            if share::is_kanzo(&format!("127.0.0.1:{port}")).await {
                println!("Kanzo is already running at {url}");
                if open {
                    open_board(&url, port);
                }
                return Ok(());
            }
            bail!("port {port} is used by another program; choose another with --port or in config.toml");
        }
        Err(e) => return Err(e).context(format!("cannot listen on {} port {port}", guard.address())),
    };
    #[cfg(windows)]
    exit_with_console();
    println!("Kanzo board at {url}, data in {}", home.dir.display());
    match guard.net() {
        Net::Private => {}
        Net::Joined(host) => println!("It also shows the board at {host}."),
        _ => {
            println!("{}", guard.status().message());
            if home.store().and_then(|s| s.has_users()).unwrap_or(false) {
                println!("It asks everyone to sign in.");
            } else {
                println!(
                    "Anyone who can reach port {port} can read and change this board: add a user in Settings to ask for a login."
                );
            }
        }
    }
    if home.config.mcp_http {
        println!("MCP over HTTP at {url}/mcp");
    }
    if open {
        // It waits to see if there's an app, which must not hold up the board.
        let url = url.clone();
        tokio::task::spawn_blocking(move || open_board(&url, port));
    }
    // Cancelling this also ends open MCP streams, which would otherwise hold up shutdown.
    let shutdown = CancellationToken::new();
    let app = router(home.clone(), guard.clone(), shutdown.clone());
    let serve = |listener, stop: CancellationToken| {
        let app = app.clone().into_make_service_with_connect_info::<SocketAddr>();
        tokio::spawn(
            axum::serve(listener, app).with_graceful_shutdown(async move { stop.cancelled().await }).into_future(),
        )
    };
    let mut running = Vec::new();
    // `localhost` is ::1 before 127.0.0.1 on many Linux systems, and not every
    // client (busybox wget, older Node) falls back, so listen on ::1 as well.
    if let IpAddr::V4(ip) = home.config.listen
        && (ip.is_loopback() || ip.is_unspecified())
        && let Ok(v6) = TcpListener::bind(SocketAddr::new(Ipv6Addr::LOCALHOST.into(), port)).await
    {
        running.push(serve(v6, shutdown.clone()));
    }
    let stopper = shutdown.clone();
    tokio::spawn(async move {
        stop_signal().await;
        stopper.cancel();
    });
    #[cfg(unix)]
    tokio::spawn(restart_when_replaced());
    let mut listener = listener;
    loop {
        let rebind = shutdown.child_token();
        *guard.rebind.lock().unwrap() = rebind.clone();
        running.push(serve(listener, rebind.clone()));
        rebind.cancelled().await;
        if shutdown.is_cancelled() {
            break;
        }
        listener = match listen_again(guard.address(), port).await {
            Ok(l) => l,
            Err(e) => {
                eprintln!("kanzo: cannot listen on {}: {e}", guard.address());
                listen_again(home.config.listen, port).await?
            }
        };
    }
    for task in running {
        task.await??;
    }
    Ok(())
}

/// However Kanzo was updated, from Settings, `kanzo update` or a package manager, the board
/// restarts on the new program, keeping its port and data folder. Open pages then reload.
#[cfg(unix)]
async fn restart_when_replaced() {
    let Some(exe) = update::Exe::current() else { return };
    let mut tick = tokio::time::interval(Duration::from_secs(3));
    loop {
        tick.tick().await;
        if !exe.changed() {
            continue;
        }
        let check = exe.clone();
        let Ok(Some(new)) = tokio::task::spawn_blocking(move || check.replacement().map(|p| p.to_path_buf())).await
        else {
            continue;
        };
        println!("Kanzo was updated; restarting the board.");
        eprintln!("kanzo: updated, but cannot restart: {}", update::restart(&new));
        return;
    }
}

/// The old listener takes a moment to let go of the port.
async fn listen_again(ip: IpAddr, port: u16) -> std::io::Result<TcpListener> {
    for _ in 0..100 {
        match TcpListener::bind(SocketAddr::new(ip, port)).await {
            Err(e) if e.kind() == std::io::ErrorKind::AddrInUse => tokio::time::sleep(Duration::from_millis(20)).await,
            bound => return bound,
        }
    }
    TcpListener::bind(SocketAddr::new(ip, port)).await
}

async fn stop_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        match signal(SignalKind::terminate()) {
            Ok(mut term) => tokio::select! {
                _ = tokio::signal::ctrl_c() => {}
                _ = term.recv() => {}
            },
            Err(_) => {
                let _ = tokio::signal::ctrl_c().await;
            }
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

fn router(home: Home, guard: Arc<Guard>, shutdown: CancellationToken) -> Router {
    // A joined board sends its agents to the shared one, while its page shows both boards.
    let mut agents = Router::new();
    if home.config.mcp_http {
        agents = agents
            .nest_service("/mcp", crate::mcp::http_service(home.clone(), shutdown))
            .route_layer(middleware::from_fn_with_state(guard.clone(), forward));
    }
    let board = Router::new()
        .route("/images/{file}", get(image_file))
        .route("/api/changes", get(changes))
        .route("/api/board", get(board))
        .route("/api/search", get(search))
        .route("/api/projects", get(projects).post(create_project))
        .route("/api/projects/{slug}", patch(update_project).delete(delete_project))
        .route("/api/projects/{slug}/repos", post(link_repo).delete(unlink_repo))
        .route("/api/projects/{slug}/move", post(move_project))
        .route("/api/tasks", post(create_task))
        .route("/api/tasks/bulk", post(bulk_tasks))
        .route("/api/tasks/move", post(move_tasks))
        .route("/api/tasks/{id}", get(get_task).patch(update_task).delete(delete_task))
        .route("/api/tasks/{id}/move", post(move_task))
        .route("/api/tasks/{id}/comments", post(add_comment))
        .route("/api/comments/{id}", delete(delete_comment))
        .route("/api/images", post(save_image).layer(DefaultBodyLimit::max(IMAGE_LIMIT)))
        .route("/api/images/{id}", delete(delete_image))
        .route("/api/tasks/{id}/images", post(attach_image))
        .route("/api/sync", get(sync_status).post(sync_now).put(sync_repo))
        .route("/api/settings", patch(update_settings))
        .route("/api/session", get(session).post(sign_in).delete(sign_out))
        .route("/api/users", get(users).post(create_user))
        .route("/api/users/{id}", patch(update_user).delete(delete_user))
        .route("/api/tokens", get(tokens).post(create_token))
        .route("/api/tokens/{id}", delete(delete_token));
    let auth = Arc::new(Auth::new(home.clone()));
    Router::new()
        .route("/", get(|| async { Html(INDEX) }))
        .route("/app.js", get(|| async { ([(header::CONTENT_TYPE, "text/javascript; charset=utf-8")], APP_JS) }))
        .route(
            "/i18n/zh-cn.js",
            get(|| async { ([(header::CONTENT_TYPE, "text/javascript; charset=utf-8")], ZH_CN_JS) }),
        )
        .route("/logo.png", get(|| async { ([(header::CONTENT_TYPE, "image/png")], LOGO) }))
        .route("/icon.png", get(|| async { ([(header::CONTENT_TYPE, "image/png")], ICON) }))
        .route("/changelog.md", get(|| async { ([(header::CONTENT_TYPE, "text/markdown; charset=utf-8")], CHANGELOG) }))
        .route(
            "/manifest.webmanifest",
            get(|| async { ([(header::CONTENT_TYPE, "application/manifest+json")], MANIFEST) }),
        )
        .route("/api/ping", get(|| async { "kanzo" }))
        .route("/api/update", get(update_status).post(update_now))
        .route("/api/network", get(network).post(change_network).route_layer(middleware::from_fn(this_computer)))
        // The files are on this computer, whichever board the task is on.
        .route("/api/open", post(open_file).route_layer(middleware::from_fn(this_computer)))
        .route("/remote/{*path}", any(remote))
        .merge(board)
        .merge(agents)
        .layer(Extension(guard.clone()))
        .layer(Extension(auth.clone()))
        // Inside `protect`: a request from another site never gets as far as a login.
        .layer(middleware::from_fn_with_state(auth, authenticate))
        .layer(middleware::from_fn_with_state(guard, protect))
        .layer(middleware::map_response(stamp_version))
        .with_state(Arc::new(home))
}

/// Pages compare it with the version they loaded with, and reload after an update. Set on
/// the way out, so a joined board's forwarded replies carry this computer's version, not the host's.
async fn stamp_version(mut res: Response) -> Response {
    res.headers_mut().insert("x-kanzo-version", HeaderValue::from_static(update::CURRENT));
    res
}

/// DNS rebinding needs a domain name the attacker controls, so the board only accepts
/// `localhost`, IP addresses and, when it listens on the network, this machine's own names.
struct Guard {
    port: u16,
    listen: IpAddr,
    net: Mutex<Net>,
    /// The access token this board joined a board with accounts with.
    token: Mutex<Option<String>>,
    guests: Mutex<HashMap<IpAddr, (Instant, Option<String>)>>,
    names: OnceLock<Vec<String>>,
    rebind: Mutex<CancellationToken>,
}

/// A guest's board asks every 2 seconds while it's on screen.
const GUEST_GONE: Duration = Duration::from_secs(30);

impl Guard {
    fn new(port: u16, listen: IpAddr, net: Net) -> Self {
        Self {
            port,
            listen,
            net: Mutex::new(net),
            token: Mutex::default(),
            guests: Mutex::default(),
            names: OnceLock::new(),
            rebind: Mutex::default(),
        }
    }

    fn net(&self) -> Net {
        self.net.lock().unwrap().clone()
    }

    fn token(&self) -> Option<String> {
        self.token.lock().unwrap().clone()
    }

    fn with_token(self, token: Option<String>) -> Self {
        *self.token.lock().unwrap() = token;
        self
    }

    fn lan(&self) -> bool {
        self.net() == Net::Sharing
    }

    fn address(&self) -> IpAddr {
        if self.lan() && self.listen.is_loopback() { Ipv4Addr::UNSPECIFIED.into() } else { self.listen }
    }

    fn set(&self, net: Net) {
        let before = self.address();
        *self.net.lock().unwrap() = net;
        if self.address() != before {
            self.rebind.lock().unwrap().cancel();
        }
        if !self.lan() {
            self.guests.lock().unwrap().clear();
        }
    }

    fn guests(&self) -> Vec<share::Guest> {
        let mut guests = self.guests.lock().unwrap();
        guests.retain(|_, (seen, _)| seen.elapsed() < GUEST_GONE);
        let mut list: Vec<share::Guest> =
            guests.iter().map(|(ip, (_, name))| share::Guest { name: name.clone(), address: *ip }).collect();
        list.sort_by_key(|g| g.address);
        list
    }

    fn status(&self) -> share::Status {
        share::Status::new(&self.net(), self.port, self.guests())
    }

    fn names(&self) -> &[String] {
        self.names.get_or_init(|| {
            use std::process::Command;
            let run = |cmd: &mut Command| crate::home::output(cmd).ok().map(|n| n.to_ascii_lowercase());
            // macOS keeps its Bonjour name apart; `hostname` there can be a DHCP address.
            let found =
                [run(Command::new("scutil").args(["--get", "LocalHostName"])), run(&mut Command::new("hostname"))];
            let mut names = Vec::new();
            for full in found.into_iter().flatten().filter(|n| !n.is_empty() && n.parse::<IpAddr>().is_err()) {
                let short = full.split('.').next().unwrap_or_default().to_string();
                for name in [format!("{short}.local"), full, short] {
                    if !names.contains(&name) {
                        names.push(name);
                    }
                }
            }
            names
        })
    }

    fn allows(&self, host: &str) -> bool {
        let (name, port) = match host.rsplit_once(':') {
            Some((n, p)) if !p.contains(']') => (n, Some(p)),
            _ => (host, None),
        };
        if port.is_some_and(|p| p.parse() != Ok(self.port)) {
            return false;
        }
        let name = name.trim_start_matches('[').trim_end_matches(']').trim_end_matches('.');
        let ip = name.parse::<IpAddr>().ok();
        if name.eq_ignore_ascii_case("localhost") || ip.is_some_and(|ip| ip.is_loopback()) {
            return true;
        }
        self.lan() && (ip.is_some() || self.names().iter().any(|n| n.eq_ignore_ascii_case(name)))
    }
}

fn guest(req: &Request) -> Option<IpAddr> {
    req.extensions().get::<ConnectInfo<SocketAddr>>().map(|c| c.0.ip()).filter(|ip| !ip.is_loopback())
}

async fn forward(State(guard): State<Arc<Guard>>, req: Request, next: Next) -> Response {
    match guard.net() {
        Net::Joined(host) => share::forward(&host, guard.token().as_deref(), req).await,
        _ => next.run(req).await,
    }
}

/// The shared board, for the page of a board that joined it: `/remote/api/board` is its `/api/board`.
async fn remote(Extension(guard): Extension<Arc<Guard>>, mut req: Request) -> Response {
    let Net::Joined(host) = guard.net() else {
        return fail(StatusCode::NOT_FOUND, "this board has not joined another").into_response();
    };
    let path = req.uri().path_and_query().map_or("/", |p| p.as_str());
    let path = path.strip_prefix("/remote").unwrap_or(path);
    match path.parse() {
        Ok(uri) => *req.uri_mut() = uri,
        Err(e) => return fail(StatusCode::BAD_REQUEST, e).into_response(),
    }
    share::forward(&host, guard.token().as_deref(), req).await
}

async fn this_computer(req: Request, next: Next) -> Response {
    match guest(&req) {
        Some(_) => fail(StatusCode::FORBIDDEN, "only the computer with the board can do that").into_response(),
        None => next.run(req).await,
    }
}

async fn protect(State(guard): State<Arc<Guard>>, req: Request, next: Next) -> Response {
    let guest = guest(&req);
    let ok = {
        let header = |name| req.headers().get(name).and_then(|h: &HeaderValue| h.to_str().ok());
        let host = header(header::HOST).unwrap_or("");
        let same_origin = |o: &str| {
            o.len() == host.len() + 7
                && o.as_bytes()[..7].eq_ignore_ascii_case(b"http://")
                && o.as_bytes()[7..].eq_ignore_ascii_case(host.as_bytes())
        };
        guard.allows(host) && header(header::ORIGIN).is_none_or(same_origin) && (guest.is_none() || guard.lan())
    };
    if let Some(ip) = guest.filter(|_| ok) {
        let name =
            req.headers().get(share::COMPUTER_HEADER).map(|v| String::from_utf8_lossy(v.as_bytes()).into_owned());
        let mut guests = guard.guests.lock().unwrap();
        let guest = guests.entry(ip).or_insert((Instant::now(), None));
        guest.0 = Instant::now();
        if name.is_some() {
            guest.1 = name;
        }
    }
    let mut res = if ok {
        next.run(req).await
    } else {
        (StatusCode::FORBIDDEN, "kanzo does not answer requests from other sites").into_response()
    };
    let headers = res.headers_mut();
    headers.insert(header::CONTENT_SECURITY_POLICY, HeaderValue::from_static(CSP));
    headers.insert(header::X_CONTENT_TYPE_OPTIONS, HeaderValue::from_static("nosniff"));
    headers.insert(header::REFERRER_POLICY, HeaderValue::from_static("no-referrer"));
    headers.entry(header::CACHE_CONTROL).or_insert(HeaderValue::from_static("no-cache"));
    res
}

type Shared = State<Arc<Home>>;

async fn with_store<T: Serialize + Send + 'static>(
    home: Arc<Home>,
    f: impl FnOnce(&Home, &Store) -> store::Result<T> + Send + 'static,
) -> Response {
    match blocking(move || f(&home, &home.store()?)).await {
        Ok(Ok(v)) => Json(v).into_response(),
        Ok(Err(e)) => Fail::from(e).into_response(),
        Err(e) => e.into_response(),
    }
}

impl From<Error> for Fail {
    fn from(error: Error) -> Self {
        let code = match error {
            Error::NotFound(_) => StatusCode::NOT_FOUND,
            Error::Invalid(_) => StatusCode::BAD_REQUEST,
            Error::Busy(_) => StatusCode::SERVICE_UNAVAILABLE,
            Error::Db(_) => StatusCode::INTERNAL_SERVER_ERROR,
        };
        fail(code, error)
    }
}

type Reply = Result<Response, Fail>;

pub struct Fail(StatusCode, String);

pub fn fail(code: StatusCode, error: impl std::fmt::Display) -> Fail {
    Fail(code, error.to_string())
}

impl IntoResponse for Fail {
    fn into_response(self) -> Response {
        (self.0, Json(json!({ "error": self.1 }))).into_response()
    }
}

async fn blocking<T: Send + 'static>(f: impl FnOnce() -> T + Send + 'static) -> Result<T, Fail> {
    tokio::task::spawn_blocking(f).await.map_err(|e| fail(StatusCode::INTERNAL_SERVER_ERROR, e))
}

/// The database file's identity is part of it so a board pulled by `kanzo sync` always reloads.
fn change_token(home: &Home, s: &Store) -> store::Result<String> {
    Ok(format!("{}-{}", s.changes()?, home.db_identity()))
}

#[derive(Serialize)]
struct Changes {
    changes: String,
    guests: Option<usize>,
    /// The shared board this one joined, so its page shows it, or stops, from the next check.
    joined: Option<String>,
}

async fn changes(State(home): Shared, Extension(guard): Extension<Arc<Guard>>) -> Response {
    let guests = guard.lan().then(|| guard.guests().len());
    let joined = match guard.net() {
        Net::Joined(host) => Some(host),
        _ => None,
    };
    with_store(home, move |h, s| Ok(Changes { changes: change_token(h, s)?, guests, joined })).await
}

#[derive(Serialize)]
struct Projects {
    changes: String,
    projects: Vec<store::Project>,
}

/// The sidebar of a board that joined another lists both boards' projects, without their tasks.
async fn projects(State(home): Shared) -> Response {
    with_store(home, move |h, s| Ok(Projects { changes: change_token(h, s)?, projects: s.list_projects()? })).await
}

/// The done column only grows, and every change reloads the board.
const DONE_SHOWN: usize = 50;

#[derive(Deserialize)]
struct BoardQuery {
    project: Option<String>,
    done: Option<String>,
    #[serde(default)]
    q: String,
}

#[derive(Serialize)]
struct Board {
    changes: String,
    done_hidden: usize,
    hidden_properties: Vec<String>,
    projects: Vec<store::Project>,
    statuses: Vec<String>,
    tasks: Vec<store::Task>,
    /// To name and picture assignees and comment authors. Left out on a board without accounts.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    users: Vec<User>,
    version: &'static str,
}

async fn board(State(home): Shared, Query(q): Query<BoardQuery>) -> Response {
    with_store(home, move |h, s| {
        let project = match q.project.as_deref().filter(|p| !p.is_empty() && *p != "all") {
            Some(slug) => Some(s.find_project(slug)?),
            None => None,
        };
        let keep_done = (q.done.as_deref() != Some("all")).then_some(DONE_SHOWN);
        let (mut tasks, done_hidden) = s.list_tasks_keeping_done(project.map(|p| p.id), None, keep_done)?;
        if !q.q.trim().is_empty() {
            let found: HashSet<i64> = s.search(&q.q, None)?.into_iter().map(|t| t.id).collect();
            tasks.retain(|t| found.contains(&t.id));
        }
        Ok(Board {
            changes: change_token(h, s)?,
            done_hidden,
            hidden_properties: s.hidden_properties()?,
            projects: s.list_projects()?,
            statuses: s.statuses().to_vec(),
            tasks,
            users: s.list_users()?,
            version: update::CURRENT,
        })
    })
    .await
}

#[derive(Deserialize)]
struct SearchQuery {
    #[serde(default)]
    q: String,
}

async fn search(State(home): Shared, Query(q): Query<SearchQuery>) -> Response {
    with_store(home, move |_, s| s.search(&q.q, Some(30))).await
}

#[derive(Deserialize)]
struct NewProject {
    slug: String,
    #[serde(default)]
    name: String,
    repo: Option<String>,
}

async fn create_project(State(home): Shared, Json(p): Json<NewProject>) -> Response {
    with_store(home, move |_, s| s.create_project(&p.slug, &p.name, p.repo.as_deref())).await
}

#[derive(Deserialize)]
struct ProjectPatch {
    name: Option<String>,
    color: Option<String>,
}

async fn update_project(State(home): Shared, Path(slug): Path<String>, Json(p): Json<ProjectPatch>) -> Response {
    with_store(home, move |_, s| {
        let mut project = None;
        if let Some(name) = &p.name {
            project = Some(s.rename_project(&slug, name)?);
        }
        if let Some(color) = &p.color {
            project = Some(s.set_project_color(&slug, color)?);
        }
        project.map_or_else(|| s.find_project(&slug), Ok)
    })
    .await
}

#[derive(Deserialize)]
struct ProjectMove {
    before: Option<String>,
}

async fn move_project(State(home): Shared, Path(slug): Path<String>, Json(m): Json<ProjectMove>) -> Response {
    with_store(home, move |_, s| s.move_project(&slug, m.before.as_deref())).await
}

#[derive(Deserialize)]
struct RepoLink {
    repo: String,
}

async fn link_repo(State(home): Shared, Path(slug): Path<String>, Json(r): Json<RepoLink>) -> Response {
    with_store(home, move |_, s| s.link_repo(&slug, &r.repo)).await
}

async fn unlink_repo(State(home): Shared, Path(slug): Path<String>, Json(r): Json<RepoLink>) -> Response {
    with_store(home, move |_, s| s.unlink_repo(&slug, &r.repo)).await
}

async fn delete_project(State(home): Shared, Path(slug): Path<String>) -> Response {
    with_store(home, move |h, s| {
        let project = s.find_project(&slug)?;
        h.deleting_images(s, ImagesOf::Project(project.id), || s.delete_project(&slug))
    })
    .await
}

#[derive(Deserialize)]
struct NewTask {
    project: String,
    title: String,
    description: Option<String>,
    assignee: Option<i64>,
}

async fn create_task(State(home): Shared, Json(t): Json<NewTask>) -> Response {
    with_store(home, move |_, s| {
        let project = s.find_project(&t.project)?;
        let patch = TaskPatch { description: t.description, assignee: t.assignee, ..Default::default() };
        s.create_task_in(project.id, &t.title, patch)
    })
    .await
}

#[derive(Deserialize)]
#[serde(untagged)]
enum Settings {
    Backlog { backlog: bool },
    Property { property: String, visible: bool },
}

async fn update_settings(State(home): Shared, Json(r): Json<Settings>) -> Response {
    with_store(home, move |_, s| match r {
        Settings::Backlog { backlog } => s.set_backlog(backlog),
        Settings::Property { property, visible } => s.set_property(&property, visible),
    })
    .await
}

#[derive(Serialize)]
struct TaskReply {
    blocks: Vec<store::TaskLink>,
    comments: Vec<store::Comment>,
    images: Vec<store::Image>,
    task: store::Task,
    waits_on: Vec<store::TaskLink>,
}

async fn get_task(State(home): Shared, Path(id): Path<i64>) -> Response {
    with_store(home, move |_, s| {
        Ok(TaskReply {
            blocks: s.task_links(id, true)?,
            comments: s.list_comments(id)?,
            images: s.list_images(id)?,
            task: s.get_task(id)?,
            waits_on: s.task_links(id, false)?,
        })
    })
    .await
}

#[derive(Deserialize)]
struct Patch {
    title: Option<String>,
    description: Option<String>,
    status: Option<String>,
    project: Option<String>,
    tags: Option<String>,
    human: Option<bool>,
    url: Option<String>,
    waits_on: Option<String>,
    /// A user's id; 0 assigns the task to nobody.
    assignee: Option<i64>,
}

async fn update_task(State(home): Shared, Path(id): Path<i64>, Json(p): Json<Patch>) -> Response {
    with_store(home, move |_, s| {
        let project_id = p.project.map(|slug| s.find_project(&slug)).transpose()?.map(|p| p.id);
        s.update_task(
            id,
            TaskPatch {
                title: p.title,
                description: p.description,
                status: p.status,
                project_id,
                tags: p.tags,
                human: p.human,
                url: p.url,
                waits_on: p.waits_on,
                assignee: p.assignee,
            },
        )
    })
    .await
}

#[derive(Deserialize)]
struct Move {
    status: String,
    before: Option<i64>,
}

async fn move_task(State(home): Shared, Path(id): Path<i64>, Json(m): Json<Move>) -> Response {
    with_store(home, move |_, s| s.move_task(id, &m.status, m.before)).await
}

#[derive(Deserialize)]
struct MoveMany {
    ids: Vec<i64>,
    status: String,
    before: Option<i64>,
}

async fn move_tasks(State(home): Shared, Json(m): Json<MoveMany>) -> Response {
    with_store(home, move |_, s| s.move_tasks(&m.ids, &m.status, m.before)).await
}

async fn delete_task(State(home): Shared, Path(id): Path<i64>) -> Response {
    with_store(home, move |h, s| h.deleting_images(s, ImagesOf::Task(id), || s.delete_task(id))).await
}

#[derive(Deserialize)]
struct Bulk {
    ids: Vec<i64>,
    status: Option<String>,
    #[serde(default)]
    add_tags: String,
    #[serde(default)]
    remove_tags: String,
    #[serde(default)]
    delete: bool,
}

async fn bulk_tasks(State(home): Shared, Json(b): Json<Bulk>) -> Response {
    with_store(home, move |h, s| {
        if b.delete {
            h.deleting_images(s, ImagesOf::Tasks(b.ids.clone()), || s.delete_tasks(&b.ids))
        } else {
            s.update_tasks(&b.ids, b.status.as_deref(), &b.add_tags, &b.remove_tags)
        }
    })
    .await
}

#[derive(Deserialize)]
struct NewComment {
    #[serde(default)]
    body: String,
    #[serde(default)]
    images: Vec<String>,
}

async fn add_comment(
    State(home): Shared,
    Extension(who): Extension<Who>,
    Path(id): Path<i64>,
    Json(c): Json<NewComment>,
) -> Response {
    with_store(home, move |h, s| {
        for file in &c.images {
            image_saved(h, file)?;
        }
        let author = who.user.as_ref().map_or("me", |u| u.name.as_str());
        s.add_comment_by(id, author, who.user.as_ref().map(|u| u.id), &c.body, &c.images)
    })
    .await
}

#[derive(Deserialize)]
struct Attach {
    file: String,
}

async fn attach_image(State(home): Shared, Path(id): Path<i64>, Json(a): Json<Attach>) -> Response {
    with_store(home, move |h, s| {
        image_saved(h, &a.file)?;
        s.add_image(id, &a.file)
    })
    .await
}

async fn delete_image(State(home): Shared, Path(id): Path<i64>) -> Response {
    with_store(home, move |h, s| h.deleting_images(s, ImagesOf::Image(id), || s.delete_image(id))).await
}

fn image_saved(home: &Home, file: &str) -> store::Result<()> {
    if store::valid_image_file(file) && home.images().join(file).is_file() {
        Ok(())
    } else {
        Err(Error::Invalid(format!("no saved image {file:?}; paste it again")))
    }
}

/// Named by its content's hash, so it never changes and can be cached for good.
async fn image_file(State(home): Shared, Path(file): Path<String>) -> Response {
    let ext = file.rsplit_once('.').map(|(_, ext)| ext);
    let Some((_, mime)) = store::IMAGE_TYPES.iter().find(|(e, _)| Some(*e) == ext) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    if !store::valid_image_file(&file) {
        return StatusCode::NOT_FOUND.into_response();
    }
    let path = home.images().join(&file);
    match tokio::task::spawn_blocking(move || std::fs::read(path)).await {
        Ok(Ok(bytes)) => {
            ([(header::CONTENT_TYPE, *mime), (header::CACHE_CONTROL, "public, max-age=31536000, immutable")], bytes)
                .into_response()
        }
        _ => StatusCode::NOT_FOUND.into_response(),
    }
}

async fn delete_comment(State(home): Shared, Path(id): Path<i64>) -> Response {
    with_store(home, move |h, s| h.deleting_images(s, ImagesOf::Comment(id), || s.delete_comment(id))).await
}

async fn save_image(State(home): Shared, headers: HeaderMap, body: Bytes) -> Reply {
    let mime = headers.get(header::CONTENT_TYPE).and_then(|v| v.to_str().ok());
    let Some((ext, _)) = store::IMAGE_TYPES.iter().find(|(_, m)| Some(*m) == mime) else {
        return Err(fail(StatusCode::BAD_REQUEST, "paste a PNG, JPEG, GIF or WebP image"));
    };
    if body.is_empty() {
        return Err(fail(StatusCode::BAD_REQUEST, "the image is empty"));
    }
    let path = blocking(move || -> std::io::Result<std::path::PathBuf> {
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        body.hash(&mut hasher);
        let path = home.images().join(format!("{:016x}.{ext}", hasher.finish()));
        if !path.exists() {
            crate::home::write_whole(&path, &body)?;
        }
        Ok(path)
    })
    .await?
    .map_err(|e| fail(StatusCode::INTERNAL_SERVER_ERROR, format!("cannot save the image: {e}")))?;
    let file = path.file_name().map(|f| f.to_string_lossy().into_owned()).unwrap_or_default();
    Ok(Json(json!({ "file": file, "path": path.display().to_string() })).into_response())
}

/// Asked on every request, so what it learns from the database is kept for a moment. A change
/// made on this board forgets it at once; one made from the terminal, with `kanzo user`,
/// takes up to `REMEMBERED` to show.
struct Auth {
    home: Home,
    seen: Mutex<Seen>,
    failures: Mutex<HashMap<IpAddr, (u32, Instant)>>,
}

#[derive(Default)]
struct Seen {
    accounts: Option<(bool, Instant)>,
    /// By the digest of a session or an access token.
    users: HashMap<String, (Option<User>, Instant)>,
}

const REMEMBERED: Duration = Duration::from_secs(10);
/// Wrong passwords one address can send before it waits.
const FAILURES: u32 = 10;
const FAILURE_WINDOW: Duration = Duration::from_secs(5 * 60);

impl Auth {
    fn new(home: Home) -> Self {
        Self { home, seen: Mutex::default(), failures: Mutex::default() }
    }

    fn forget(&self) {
        *self.seen.lock().unwrap() = Seen::default();
    }

    async fn accounts(&self) -> Result<bool, Fail> {
        if let Some((on, at)) = self.seen.lock().unwrap().accounts
            && at.elapsed() < REMEMBERED
        {
            return Ok(on);
        }
        let home = self.home.clone();
        let on = blocking(move || home.store()?.has_users()).await??;
        self.seen.lock().unwrap().accounts = Some((on, Instant::now()));
        Ok(on)
    }

    async fn user(&self, digest: String, session: bool) -> Result<Option<User>, Fail> {
        {
            let mut seen = self.seen.lock().unwrap();
            seen.users.retain(|_, (_, at)| at.elapsed() < REMEMBERED);
            if let Some((user, _)) = seen.users.get(&digest) {
                return Ok(user.clone());
            }
        }
        let (home, key) = (self.home.clone(), digest.clone());
        let user = blocking(move || {
            let store = home.store()?;
            if session { store.session_user(&key) } else { store.token_user(&key) }
        })
        .await??;
        self.seen.lock().unwrap().users.insert(digest, (user.clone(), Instant::now()));
        Ok(user)
    }

    fn locked_out(&self, ip: IpAddr) -> bool {
        let mut failures = self.failures.lock().unwrap();
        failures.retain(|_, (_, since)| since.elapsed() < FAILURE_WINDOW);
        failures.get(&ip).is_some_and(|(count, _)| *count >= FAILURES)
    }

    fn failed(&self, ip: IpAddr) {
        self.failures.lock().unwrap().entry(ip).or_insert((0, Instant::now())).0 += 1;
    }
}

impl Who {
    fn user(&self) -> Result<&User, Fail> {
        self.user.as_ref().ok_or_else(|| fail(StatusCode::UNAUTHORIZED, "sign in to do that"))
    }

    /// On a board without accounts everyone may do what an admin does, as before.
    fn admin(&self) -> Result<(), Fail> {
        match &self.user {
            Some(user) if !user.admin => Err(fail(StatusCode::FORBIDDEN, "only an admin can do that")),
            None if self.accounts => Err(fail(StatusCode::UNAUTHORIZED, "sign in to do that")),
            _ => Ok(()),
        }
    }
}

fn cookie<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    let all = headers.get_all(header::COOKIE);
    let mut pairs = all.iter().filter_map(|v| v.to_str().ok()).flat_map(|v| v.split(';'));
    pairs.find_map(|pair| pair.trim().strip_prefix(name)?.strip_prefix('='))
}

fn bearer(headers: &HeaderMap) -> Option<&str> {
    let value = headers.get(header::AUTHORIZATION)?.to_str().ok()?;
    let (scheme, token) = value.split_once(' ')?;
    scheme.eq_ignore_ascii_case("bearer").then(|| token.trim())
}

/// What anyone may load: the page that asks for the login, and what it needs to ask.
fn open_to_all(req: &Request) -> bool {
    let path = req.uri().path();
    // `kanzo share` and `kanzo join` ask the board on this computer, with no login to give. A
    // proxy on this computer would make every visitor look local, so what it forwards doesn't count.
    if path == "/api/network" || path == "/api/open" {
        let forwarded = ["forwarded", "x-forwarded-for", "x-real-ip"].iter().any(|h| req.headers().contains_key(*h));
        return guest(req).is_none() && !forwarded;
    }
    const OPEN: [&str; 8] = [
        "/",
        "/app.js",
        "/logo.png",
        "/icon.png",
        "/manifest.webmanifest",
        "/changelog.md",
        "/api/ping",
        "/api/session",
    ];
    OPEN.contains(&path) || path.starts_with("/i18n/")
}

/// Finds who is asking, from a browser's session cookie or an agent's access token, and on
/// a board with accounts turns away everyone else.
async fn authenticate(State(auth): State<Arc<Auth>>, mut req: Request, next: Next) -> Response {
    // Digests, not the headers: a request can't be borrowed across the waits below.
    let token = bearer(req.headers()).map(auth::digest);
    let session = cookie(req.headers(), auth::COOKIE).map(auth::digest);
    let found = async {
        let accounts = auth.accounts().await?;
        let mut who = Who { accounts, ..Default::default() };
        if !accounts {
            return Ok(who);
        }
        if let Some(token) = token {
            who.user = auth.user(token, false).await?;
        } else if let Some(session) = session {
            who.user = auth.user(session.clone(), true).await?;
            who.session = who.user.is_some().then_some(session);
        }
        Ok::<_, Fail>(who)
    };
    let who = match found.await {
        Ok(who) => who,
        Err(e) => return e.into_response(),
    };
    if who.accounts && who.user.is_none() && !open_to_all(&req) {
        let mut res = (StatusCode::UNAUTHORIZED, Json(json!({ "error": "sign in to use this board", "login": true })))
            .into_response();
        res.headers_mut().insert(header::WWW_AUTHENTICATE, HeaderValue::from_static("Bearer"));
        return res;
    }
    req.extensions_mut().insert(who);
    next.run(req).await
}

fn session_cookie(value: &str, seconds: u32) -> HeaderValue {
    // No `Secure`: the board is plain HTTP on a home network. `HttpOnly` keeps it from scripts.
    let cookie = format!("{}={value}; Path=/; HttpOnly; SameSite=Lax; Max-Age={seconds}", auth::COOKIE);
    HeaderValue::from_str(&cookie).expect("hex and digits only")
}

const SESSION_SECONDS: u32 = 30 * 24 * 60 * 60;

#[derive(Serialize)]
struct Session {
    /// False until the board has its first user: there is then nobody to sign in as.
    accounts: bool,
    user: Option<User>,
}

async fn session(Extension(who): Extension<Who>) -> Response {
    Json(Session { accounts: who.accounts, user: who.user }).into_response()
}

/// Signs `user` in from this browser.
async fn start_session(home: Arc<Home>, auth: &Auth, user: User) -> Reply {
    let secret = auth::new_session();
    let (digest, id) = (auth::digest(&secret), user.id);
    blocking(move || home.store()?.create_session(id, &digest)).await??;
    auth.forget();
    let mut res = Json(Session { accounts: true, user: Some(user) }).into_response();
    res.headers_mut().insert(header::SET_COOKIE, session_cookie(&secret, SESSION_SECONDS));
    Ok(res)
}

#[derive(Deserialize)]
struct Login {
    name: String,
    password: String,
}

async fn sign_in(
    State(home): Shared,
    Extension(auth): Extension<Arc<Auth>>,
    peer: Option<Extension<ConnectInfo<SocketAddr>>>,
    Json(login): Json<Login>,
) -> Reply {
    let ip = peer.map_or(IpAddr::V4(Ipv4Addr::UNSPECIFIED), |c| c.0.0.ip());
    if auth.locked_out(ip) {
        return Err(fail(StatusCode::TOO_MANY_REQUESTS, "too many wrong passwords: wait a few minutes"));
    }
    let checking = home.clone();
    let user = blocking(move || -> store::Result<Option<User>> {
        let found = checking.store()?.login(&login.name)?;
        // The store is closed before the slow part, so the check holds no lock on the board.
        Ok(match found {
            Some((user, hash)) => auth::verify_password(&login.password, &hash).then_some(user),
            None => {
                auth::waste_a_check(&login.password);
                None
            }
        })
    })
    .await??;
    match user {
        Some(user) => start_session(home, &auth, user).await,
        None => {
            auth.failed(ip);
            Err(fail(StatusCode::UNAUTHORIZED, "Wrong name or password."))
        }
    }
}

async fn sign_out(State(home): Shared, Extension(auth): Extension<Arc<Auth>>, Extension(who): Extension<Who>) -> Reply {
    if let Some(session) = who.session {
        blocking(move || home.store()?.delete_session(&session)).await??;
        auth.forget();
    }
    let mut res = Json(json!({})).into_response();
    res.headers_mut().insert(header::SET_COOKIE, session_cookie("", 0));
    Ok(res)
}

async fn users(State(home): Shared) -> Response {
    with_store(home, |_, s| s.list_users()).await
}

#[derive(Deserialize)]
struct NewUser {
    name: String,
    password: String,
    #[serde(default)]
    admin: bool,
}

/// The first user turns accounts on, runs the board and is signed in. After that only an admin adds users.
async fn create_user(
    State(home): Shared,
    Extension(auth): Extension<Arc<Auth>>,
    Extension(who): Extension<Who>,
    Json(new): Json<NewUser>,
) -> Reply {
    who.admin()?;
    auth::check_password(&new.password).map_err(|e| fail(StatusCode::BAD_REQUEST, e))?;
    let first = !who.accounts;
    let creating = home.clone();
    let user = blocking(move || {
        let hash = auth::hash_password(&new.password);
        let store = creating.store()?;
        // Asked again in case two browsers set the board up at once: only one is the first.
        if first && store.has_users()? {
            return Err(Error::Invalid("this board already has a user: sign in".into()));
        }
        store.create_user(&new.name, &hash, new.admin)
    })
    .await??;
    auth.forget();
    if first { start_session(home, &auth, user).await } else { Ok(Json(user).into_response()) }
}

/// Tells a field left out from one set to null, which takes the picture away.
fn nullable<'de, D: serde::Deserializer<'de>>(d: D) -> Result<Option<Option<String>>, D::Error> {
    Option::<String>::deserialize(d).map(Some)
}

#[derive(Deserialize)]
struct UserChange {
    name: Option<String>,
    password: Option<String>,
    /// Needed to change your own password.
    current_password: Option<String>,
    admin: Option<bool>,
    #[serde(default, deserialize_with = "nullable")]
    avatar: Option<Option<String>>,
}

/// Users change their own name, password and picture; an admin changes anyone's, and who is an admin.
async fn update_user(
    State(home): Shared,
    Extension(auth): Extension<Arc<Auth>>,
    Extension(who): Extension<Who>,
    Path(id): Path<i64>,
    Json(change): Json<UserChange>,
) -> Reply {
    let me = who.user()?.clone();
    let own = me.id == id;
    if !own && !me.admin {
        return Err(fail(StatusCode::FORBIDDEN, "only an admin can change another user"));
    }
    if change.admin.is_some() && !me.admin {
        return Err(fail(StatusCode::FORBIDDEN, "only an admin can make someone an admin"));
    }
    if let Some(password) = &change.password {
        auth::check_password(password).map_err(|e| fail(StatusCode::BAD_REQUEST, e))?;
    }
    let session = who.session.clone();
    let updated = blocking(move || -> store::Result<Result<User, &'static str>> {
        if let Some(Some(file)) = &change.avatar {
            image_saved(&home, file)?;
        }
        let mut password = None;
        if let Some(new) = &change.password {
            if own {
                let stored = home.store()?.login(&me.name)?.map(|(_, hash)| hash).unwrap_or_default();
                if !auth::verify_password(change.current_password.as_deref().unwrap_or(""), &stored) {
                    return Ok(Err("your current password is wrong"));
                }
            }
            password = Some(auth::hash_password(new));
        }
        let store = home.store()?;
        let patch = UserPatch { name: change.name, password, admin: change.admin, avatar: change.avatar };
        let (user, old_picture) = store.update_user(id, patch, session.as_deref())?;
        drop_unused(&home, &store, old_picture)?;
        Ok(Ok(user))
    })
    .await??;
    auth.forget();
    match updated {
        Ok(user) => Ok(Json(user).into_response()),
        Err(wrong) => Err(fail(StatusCode::FORBIDDEN, wrong)),
    }
}

/// A picture nothing shows any more leaves the data folder.
fn drop_unused(home: &Home, store: &Store, file: Option<String>) -> store::Result<()> {
    for file in store.unused_images(file.into_iter().collect())? {
        let _ = std::fs::remove_file(home.images().join(file));
    }
    Ok(())
}

async fn delete_user(
    State(home): Shared,
    Extension(auth): Extension<Arc<Auth>>,
    Extension(who): Extension<Who>,
    Path(id): Path<i64>,
) -> Reply {
    who.admin()?;
    if who.user()?.id == id {
        return Err(fail(StatusCode::BAD_REQUEST, "you can't remove yourself: another admin can"));
    }
    let user = blocking(move || {
        let store = home.store()?;
        let user = store.delete_user(id)?;
        drop_unused(&home, &store, user.avatar.clone())?;
        Ok::<_, Error>(user)
    })
    .await??;
    auth.forget();
    Ok(Json(user).into_response())
}

async fn tokens(State(home): Shared, Extension(who): Extension<Who>) -> Reply {
    let id = who.user()?.id;
    Ok(with_store(home, move |_, s| s.list_tokens(id)).await)
}

#[derive(Deserialize)]
struct NewToken {
    name: String,
}

/// The only time the token itself is shown: the board keeps its digest.
async fn create_token(State(home): Shared, Extension(who): Extension<Who>, Json(new): Json<NewToken>) -> Reply {
    let id = who.user()?.id;
    let secret = auth::new_token();
    let digest = auth::digest(&secret);
    let token = blocking(move || home.store()?.create_token(id, &new.name, &digest)).await??;
    Ok(Json(json!({ "id": token.id, "name": token.name, "created_at": token.created_at, "token": secret }))
        .into_response())
}

async fn delete_token(
    State(home): Shared,
    Extension(auth): Extension<Arc<Auth>>,
    Extension(who): Extension<Who>,
    Path(id): Path<i64>,
) -> Reply {
    let user = who.user()?.id;
    blocking(move || home.store()?.delete_token(user, id)).await??;
    auth.forget();
    Ok(Json(json!({})).into_response())
}

async fn network(Extension(guard): Extension<Arc<Guard>>) -> Response {
    Json(guard.status()).into_response()
}

async fn change_network(
    State(home): Shared,
    Extension(guard): Extension<Arc<Guard>>,
    Json(change): Json<Change>,
) -> Response {
    match share::change(&home, change).await {
        Ok(net) => {
            *guard.token.lock().unwrap() = share::token(&home);
            guard.set(net);
            Json(guard.status()).into_response()
        }
        Err(e) => fail(StatusCode::BAD_REQUEST, e).into_response(),
    }
}

#[derive(Deserialize)]
struct OpenFile {
    path: std::path::PathBuf,
}

async fn open_file(State(home): Shared, Json(o): Json<OpenFile>) -> Reply {
    blocking(move || home.open_file(&o.path)).await?.map_err(|e| fail(StatusCode::BAD_REQUEST, e))?;
    Ok(Json(json!({})).into_response())
}

#[derive(Deserialize)]
struct SyncRequest {
    #[serde(default)]
    keep: Option<String>,
}

async fn sync_now(State(home): Shared, Json(r): Json<SyncRequest>) -> Reply {
    let keep = match r.keep.as_deref() {
        Some("local") => Some(sync::Keep::Local),
        Some("remote") => Some(sync::Keep::Remote),
        _ => None,
    };
    let synced = blocking(move || sync::sync(&home, keep, false)).await?;
    Ok(match synced {
        Ok(message) => Json(json!({ "message": message })).into_response(),
        Err(e) => match e.downcast_ref::<sync::Diverged>() {
            Some(d) => (StatusCode::CONFLICT, Json(json!({ "error": d.message, "diverged": true, "first": d.first })))
                .into_response(),
            None => fail(StatusCode::INTERNAL_SERVER_ERROR, format!("{e:#}")).into_response(),
        },
    })
}

async fn sync_status(State(home): Shared) -> Response {
    let remote = blocking(move || sync::remote_url(&home)).await.ok().flatten();
    Json(json!({ "remote": remote })).into_response()
}

#[derive(Deserialize)]
struct SyncRepo {
    url: String,
}

async fn sync_repo(State(home): Shared, Json(r): Json<SyncRepo>) -> Reply {
    let remote = blocking(move || sync::set_remote(&home, &r.url, false))
        .await?
        .map_err(|e| fail(StatusCode::BAD_REQUEST, format!("{e:#}")))?;
    Ok(Json(json!({ "remote": remote })).into_response())
}

#[derive(Deserialize)]
struct UpdateQuery {
    now: Option<String>,
}

async fn update_status(Query(q): Query<UpdateQuery>) -> Response {
    let now = q.now.is_some();
    let latest = blocking(move || update::latest(now)).await.ok().flatten();
    let available = latest.clone().filter(|v| update::newer(v));
    Json(json!({ "current": update::CURRENT, "latest": latest, "available": available })).into_response()
}

async fn update_now(State(home): Shared, Extension(who): Extension<Who>) -> Reply {
    who.admin()?;
    // `can_restart` runs schtasks on Windows, so it waits off the async thread too.
    let dir = home.dir.clone();
    let (restart, installed) = blocking(move || (update::can_restart(&dir), update::install())).await?;
    let exe = installed.map_err(|error| fail(StatusCode::INTERNAL_SERVER_ERROR, error))?;
    if restart {
        tokio::spawn(async move {
            // Let the answer reach the board first.
            tokio::time::sleep(Duration::from_millis(300)).await;
            // Blocking: on Windows it waits for the service to stop this process.
            if let Ok(e) = tokio::task::spawn_blocking(move || update::restart(&exe)).await {
                eprintln!("kanzo: updated, but cannot restart: {e}");
            }
        });
    }
    Ok(Json(json!({ "restarting": restart })).into_response())
}

/// Windows doesn't stop the program when its console goes away: the service's conhost
/// when the task is stopped, or the window the board was started in.
#[cfg(windows)]
fn exit_with_console() {
    unsafe extern "system" {
        fn GetConsoleProcessList(list: *mut u32, count: u32) -> u32;
    }
    // SAFETY: room for one process id, as `count` says; it returns 0 without a console.
    let attached = || unsafe { GetConsoleProcessList(&mut 0, 1) } != 0;
    if attached() {
        std::thread::spawn(move || {
            while attached() {
                std::thread::sleep(Duration::from_secs(1));
            }
            std::process::exit(0);
        });
    }
}

/// The installed app if there is one, or else a browser tab. An app opens the address it was
/// installed from, so only on the default port, where that's this board.
fn open_board(url: &str, port: u16) {
    let app = cfg!(target_os = "macos")
        && port == crate::home::DEFAULT_PORT
        && std::process::Command::new("open")
            .args(["-a", "Kanzo"])
            .stderr(std::process::Stdio::null())
            .status()
            .is_ok_and(|s| s.success());
    if !app {
        open_browser(url);
    }
}

fn open_browser(url: &str) {
    let opener = if cfg!(target_os = "macos") {
        "open"
    } else if cfg!(windows) {
        "explorer"
    } else {
        let desktop = ["DISPLAY", "WAYLAND_DISPLAY"].iter().any(|v| std::env::var_os(v).is_some_and(|v| !v.is_empty()));
        if !desktop {
            return;
        }
        "xdg-open"
    };
    if std::process::Command::new(opener).arg(url).spawn().is_err() {
        eprintln!("Open {url} in your browser.");
    }
}

#[cfg(test)]
mod tests {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    use super::*;

    #[test]
    fn changelog_is_for_this_version() {
        let heading = format!("## {}", update::CURRENT);
        assert!(
            CHANGELOG.lines().next() == Some(heading.as_str()),
            "CHANGELOG.md should start with the notes for this release"
        );
    }

    #[test]
    fn changelog_links_each_change() {
        for line in CHANGELOG.lines().filter(|l| l.starts_with("- ")) {
            assert!(
                line.ends_with(')') && line.contains("](https://github.com/emanuele-em/kanzo/"),
                "should end with a link to its pull request or commit, after any `thanks @name`: {line}"
            );
        }
    }

    #[test]
    fn csp_allows_the_inline_theme_script() {
        use base64::Engine;
        use sha2::Digest;
        let script = INDEX.split("<script>").nth(1).and_then(|s| s.split("</script>").next()).unwrap();
        let hash = base64::engine::general_purpose::STANDARD.encode(sha2::Sha256::digest(script));
        assert!(CSP.contains(&format!("'sha256-{hash}'")), "put 'sha256-{hash}' in CSP's script-src");
    }

    /// The status of asking a board to open `path`, from `from`.
    async fn open_from(home: &Home, from: Option<IpAddr>, path: &str) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let guard = Arc::new(Guard::new(port, Ipv4Addr::LOCALHOST.into(), Net::Sharing));
        let mut app = router(home.clone(), guard, CancellationToken::new());
        if let Some(ip) = from {
            app = app.layer(Extension(ConnectInfo(SocketAddr::new(ip, 1))));
        }
        tokio::spawn(async move { axum::serve(listener, app).await });
        let body = json!({ "path": path }).to_string();
        let mut conn = tokio::net::TcpStream::connect(("127.0.0.1", port)).await.unwrap();
        let head = format!(
            "POST /api/open HTTP/1.0\r\nHost: 127.0.0.1:{port}\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n",
            body.len()
        );
        conn.write_all(format!("{head}{body}").as_bytes()).await.unwrap();
        let mut reply = String::new();
        conn.read_to_string(&mut reply).await.unwrap();
        reply.split(' ').nth(1).unwrap().to_string()
    }

    #[test]
    fn only_this_computer_opens_a_file_and_only_a_file() {
        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        runtime.block_on(async {
            let tmp = tempfile::tempdir().unwrap();
            let mut home = Home::open(tmp.path().to_path_buf()).unwrap();
            home.config.editor = Some("true".into());
            let file = tmp.path().join("notes.md");
            std::fs::write(&file, "").unwrap();
            let file = file.to_str().unwrap();

            let guest = Some(IpAddr::from([192, 168, 1, 9]));
            assert_eq!(open_from(&home, guest, file).await, "403");
            assert_eq!(open_from(&home, None, "notes.md").await, "400");
            assert_eq!(open_from(&home, None, tmp.path().to_str().unwrap()).await, "400");
            assert_eq!(open_from(&home, None, &format!("{file}.missing")).await, "400");
            #[cfg(unix)]
            assert_eq!(open_from(&home, None, file).await, "200");
        });
    }

    /// A board for `home` on a free port, on the network the way `net` says.
    async fn serve_board(home: &Home, net: Net) -> u16 {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let guard = Arc::new(Guard::new(port, Ipv4Addr::LOCALHOST.into(), net));
        let app = router(home.clone(), guard, CancellationToken::new());
        tokio::spawn(
            async move { axum::serve(listener, app.into_make_service_with_connect_info::<SocketAddr>()).await },
        );
        port
    }

    /// The status and body of `GET path` on the board at `port`.
    async fn get(port: u16, path: &str) -> (u16, String) {
        let mut conn = tokio::net::TcpStream::connect(("127.0.0.1", port)).await.unwrap();
        let head = format!("GET {path} HTTP/1.0\r\nHost: 127.0.0.1:{port}\r\n\r\n");
        conn.write_all(head.as_bytes()).await.unwrap();
        let mut reply = Vec::new();
        conn.read_to_end(&mut reply).await.unwrap();
        let reply = String::from_utf8_lossy(&reply).into_owned();
        let status = reply.split(' ').nth(1).unwrap().parse().unwrap();
        let body = reply.split_once("\r\n\r\n").map_or("", |(_, b)| b).to_string();
        (status, body)
    }

    fn slugs(body: &str) -> Vec<String> {
        let value: serde_json::Value = serde_json::from_str(body).unwrap();
        value["projects"].as_array().unwrap().iter().map(|p| p["slug"].as_str().unwrap().to_string()).collect()
    }

    /// A host sharing a board with project `api`, and a guest joined to it with its own project `mine`.
    async fn joined_boards() -> (tempfile::TempDir, tempfile::TempDir, u16, u16) {
        let host_dir = tempfile::tempdir().unwrap();
        let host = Home::open(host_dir.path().to_path_buf()).unwrap();
        host.store().unwrap().create_project("api", "API", None).unwrap();
        let host_port = serve_board(&host, Net::Sharing).await;

        let guest_dir = tempfile::tempdir().unwrap();
        let guest = Home::open(guest_dir.path().to_path_buf()).unwrap();
        guest.store().unwrap().create_project("mine", "Mine", None).unwrap();
        let guest_port = serve_board(&guest, Net::Joined(format!("127.0.0.1:{host_port}"))).await;
        (host_dir, guest_dir, host_port, guest_port)
    }

    #[test]
    fn a_joined_board_keeps_showing_its_own_projects() {
        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        runtime.block_on(async {
            let (_host_dir, _guest_dir, _, guest) = joined_boards().await;
            let (status, body) = get(guest, "/api/board").await;
            assert_eq!(status, 200, "{body}");
            assert_eq!(slugs(&body), ["mine"]);
        });
    }

    #[test]
    fn a_joined_board_reaches_the_shared_one_under_remote() {
        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        runtime.block_on(async {
            let (_host_dir, _guest_dir, _, guest) = joined_boards().await;
            let (status, body) = get(guest, "/remote/api/board").await;
            assert_eq!(status, 200, "{body}");
            assert_eq!(slugs(&body), ["api"]);
            let (status, body) = get(guest, "/remote/api/projects").await;
            assert_eq!(status, 200, "{body}");
            assert_eq!(slugs(&body), ["api"]);
        });
    }

    #[test]
    fn only_a_joined_board_has_a_remote_one() {
        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        runtime.block_on(async {
            let tmp = tempfile::tempdir().unwrap();
            let home = Home::open(tmp.path().to_path_buf()).unwrap();
            let port = serve_board(&home, Net::Private).await;
            let (status, body) = get(port, "/remote/api/board").await;
            assert_eq!(status, 404, "{body}");
            assert!(body.contains("not joined"), "{body}");
        });
    }

    #[test]
    fn projects_lists_a_board_without_its_tasks() {
        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        runtime.block_on(async {
            let tmp = tempfile::tempdir().unwrap();
            let home = Home::open(tmp.path().to_path_buf()).unwrap();
            home.store().unwrap().create_project("web", "Web", None).unwrap();
            let port = serve_board(&home, Net::Private).await;
            let (status, body) = get(port, "/api/projects").await;
            assert_eq!(status, 200, "{body}");
            assert_eq!(slugs(&body), ["web"]);
            let value: serde_json::Value = serde_json::from_str(&body).unwrap();
            assert!(value["changes"].is_string(), "{body}");
            assert!(value.get("tasks").is_none(), "{body}");
        });
    }

    #[test]
    fn a_joined_board_sees_the_update_the_shared_one_can_take() {
        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        runtime.block_on(async {
            update::pretend_latest("999.0.0");
            let (_host_dir, _guest_dir, _, guest) = joined_boards().await;
            let (status, body) = get(guest, "/remote/api/update").await;
            assert_eq!(status, 200, "{body}");
            let value: serde_json::Value = serde_json::from_str(&body).unwrap();
            assert_eq!(value["current"], update::CURRENT, "{body}");
            assert_eq!(value["available"], "999.0.0", "{body}");
        });
    }

    #[test]
    fn changes_say_which_board_this_one_joined() {
        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        runtime.block_on(async {
            let (_host_dir, _guest_dir, host, guest) = joined_boards().await;
            let (_, body) = get(guest, "/api/changes").await;
            let value: serde_json::Value = serde_json::from_str(&body).unwrap();
            assert_eq!(value["joined"], format!("127.0.0.1:{host}"), "{body}");
            let (_, body) = get(host, "/api/changes").await;
            let value: serde_json::Value = serde_json::from_str(&body).unwrap();
            assert!(value["joined"].is_null(), "{body}");
        });
    }

    #[test]
    fn hosts() {
        let local = Guard::new(4242, Ipv4Addr::LOCALHOST.into(), Net::Private);
        for ok in ["localhost", "localhost:4242", "127.0.0.1:4242", "[::1]:4242", "LOCALHOST:4242"] {
            assert!(local.allows(ok), "{ok}");
        }
        for bad in ["evil.com", "evil.com:4242", "localhost:80", "192.168.1.5:4242", "", "pi.local:4242"] {
            assert!(!local.allows(bad), "{bad}");
        }

        let lan = Guard::new(4242, Ipv4Addr::LOCALHOST.into(), Net::Sharing);
        lan.names.set(vec!["pi".into(), "pi.local".into()]).unwrap();
        for ok in ["localhost:4242", "192.168.1.5:4242", "[fe80::1]:4242", "pi.local:4242", "pi:4242", "PI.LOCAL."] {
            assert!(lan.allows(ok), "{ok}");
        }
        for bad in ["evil.com:4242", "pi.evil.com:4242", "192.168.1.5:80"] {
            assert!(!lan.allows(bad), "{bad}");
        }

        lan.set(Net::Private);
        assert!(!lan.allows("192.168.1.5:4242"));
        assert_eq!(lan.address(), IpAddr::from(Ipv4Addr::LOCALHOST));
    }

    /// One request, with its headers, as a browser or an agent sends it.
    async fn call(port: u16, method: &str, path: &str, headers: &[&str], body: &str) -> (u16, String, String) {
        let mut conn = tokio::net::TcpStream::connect(("127.0.0.1", port)).await.unwrap();
        let mut head = format!(
            "{method} {path} HTTP/1.0\r\nHost: 127.0.0.1:{port}\r\nContent-Type: application/json\r\nContent-Length: {}\r\n",
            body.len()
        );
        for header in headers {
            head.push_str(&format!("{header}\r\n"));
        }
        conn.write_all(format!("{head}\r\n{body}").as_bytes()).await.unwrap();
        let mut reply = Vec::new();
        conn.read_to_end(&mut reply).await.unwrap();
        let reply = String::from_utf8_lossy(&reply).into_owned();
        let status = reply.split(' ').nth(1).unwrap().parse().unwrap();
        let (head, body) = reply.split_once("\r\n\r\n").unwrap_or((&reply, ""));
        (status, head.to_string(), body.to_string())
    }

    /// The `Cookie` header a browser sends after a reply that signed it in.
    fn signed_in(head: &str) -> String {
        let line = head.lines().find(|l| l.to_ascii_lowercase().starts_with("set-cookie:")).expect("a session cookie");
        assert!(line.contains("HttpOnly") && line.contains("SameSite=Lax"), "{line}");
        format!("Cookie: {}", line.split_once(':').unwrap().1.split(';').next().unwrap().trim())
    }

    fn json_of(body: &str) -> serde_json::Value {
        serde_json::from_str(body).unwrap_or_else(|e| panic!("{e}: {body}"))
    }

    #[tokio::test]
    async fn a_board_with_users_asks_everyone_to_sign_in() {
        let tmp = tempfile::tempdir().unwrap();
        let home = Home::open(tmp.path().to_path_buf()).unwrap();
        let project = home.store().unwrap().create_project("api", "", None).unwrap();
        let task = home.store().unwrap().create_task(project.id, "Ship it", "").unwrap();
        let port = serve_board(&home, Net::Private).await;

        // No users yet: open to all, as before accounts.
        assert_eq!(call(port, "GET", "/api/board", &[], "").await.0, 200);
        let (_, _, body) = call(port, "GET", "/api/session", &[], "").await;
        assert_eq!(json_of(&body), json!({ "accounts": false, "user": null }));
        assert_eq!(call(port, "GET", "/api/tokens", &[], "").await.0, 401, "nobody to make a token for");
        let short = r#"{"name":"Ana","password":"short"}"#;
        assert_eq!(call(port, "POST", "/api/users", &[], short).await.0, 400);

        // The first user turns logins on, and is signed in.
        let ana_login = r#"{"name":"Ana","password":"correct horse"}"#;
        let (status, head, body) = call(port, "POST", "/api/users", &[], ana_login).await;
        assert_eq!(status, 200, "{body}");
        assert_eq!(json_of(&body)["user"]["admin"], true);
        let ana = signed_in(&head);

        let (status, head, body) = call(port, "GET", "/api/board", &[], "").await;
        assert_eq!(status, 401, "{body}");
        assert!(head.to_ascii_lowercase().contains("www-authenticate: bearer"));
        assert_eq!(json_of(&body)["login"], true);
        for closed in ["/api/users", "/api/changes", "/images/0000000000000000.png", "/api/update"] {
            assert_eq!(call(port, "GET", closed, &[], "").await.0, 401, "{closed}");
        }
        assert_eq!(call(port, "POST", "/api/users", &[], r#"{"name":"Eve","password":"let me in"}"#).await.0, 401);
        for open in ["/", "/app.js", "/api/ping", "/api/session", "/logo.png", "/api/network"] {
            assert_eq!(call(port, "GET", open, &[], "").await.0, 200, "{open}");
        }
        let proxied = ["X-Forwarded-For: 203.0.113.9"];
        assert_eq!(call(port, "GET", "/api/network", &proxied, "").await.0, 401, "local only to the proxy");
        assert_eq!(call(port, "GET", "/api/board", &["Cookie: kanzo_session=made-up"], "").await.0, 401);
        let (status, _, body) = call(port, "GET", "/api/board", &[&ana], "").await;
        assert_eq!(status, 200);
        assert_eq!(json_of(&body)["users"][0]["name"], "Ana");

        // Signing in again, with the right password only.
        let wrong = r#"{"name":"Ana","password":"correct horsf"}"#;
        assert_eq!(call(port, "POST", "/api/session", &[], wrong).await.0, 401);
        assert_eq!(
            call(port, "POST", "/api/session", &[], r#"{"name":"Eve","password":"correct horse"}"#).await.0,
            401
        );
        let (status, head, _) = call(port, "POST", "/api/session", &[], ana_login).await;
        assert_eq!(status, 200);
        let second = signed_in(&head);
        assert_eq!(call(port, "DELETE", "/api/session", &[&second], "").await.0, 200);
        assert_eq!(call(port, "GET", "/api/board", &[&second], "").await.0, 401, "signed out");

        // Only an admin adds users; everyone changes their own password, knowing the old one.
        let ben_new = r#"{"name":"Ben","password":"battery staple"}"#;
        let (status, _, body) = call(port, "POST", "/api/users", &[&ana], ben_new).await;
        assert_eq!(status, 200, "{body}");
        let ben_id = json_of(&body)["id"].as_i64().unwrap();
        assert_eq!(json_of(&body)["admin"], false);
        let (_, head, _) = call(port, "POST", "/api/session", &[], ben_new).await;
        let ben = signed_in(&head);
        let ben_path = format!("/api/users/{ben_id}");
        assert_eq!(call(port, "POST", "/api/users", &[&ben], r#"{"name":"Cy","password":"long enough"}"#).await.0, 403);
        assert_eq!(call(port, "PATCH", "/api/users/1", &[&ben], r#"{"name":"Mine"}"#).await.0, 403);
        assert_eq!(call(port, "PATCH", &ben_path, &[&ben], r#"{"admin":true}"#).await.0, 403);
        assert_eq!(call(port, "DELETE", "/api/users/1", &[&ben], "").await.0, 403);
        assert_eq!(call(port, "PATCH", &ben_path, &[&ben], r#"{"password":"a new password"}"#).await.0, 403);
        let change = r#"{"password":"a new password","current_password":"battery staple"}"#;
        assert_eq!(call(port, "PATCH", &ben_path, &[&ben], change).await.0, 200);
        assert_eq!(call(port, "GET", "/api/board", &[&ben], "").await.0, 200, "the browser that changed it stays in");
        assert_eq!(call(port, "POST", "/api/session", &[], ben_new).await.0, 401, "the old password is gone");
        assert_eq!(call(port, "DELETE", "/api/users/1", &[&ana], "").await.0, 400, "not yourself");

        // A comment is its writer's, and a task takes an assignee.
        let comments = format!("/api/tasks/{}/comments", task.id);
        let (status, _, body) = call(port, "POST", &comments, &[&ben], r#"{"body":"On it"}"#).await;
        assert_eq!(status, 200, "{body}");
        assert_eq!(
            (json_of(&body)["author"].as_str(), json_of(&body)["user_id"].as_i64()),
            (Some("Ben"), Some(ben_id))
        );
        let assign = format!(r#"{{"assignee":{ben_id}}}"#);
        let (_, _, body) = call(port, "PATCH", &format!("/api/tasks/{}", task.id), &[&ana], &assign).await;
        assert_eq!(json_of(&body)["assignee"], ben_id);

        // An agent signs in with an access token, and its comments count as its user's.
        let (status, _, body) = call(port, "POST", "/api/tokens", &[&ana], r#"{"name":"Claude Code"}"#).await;
        assert_eq!(status, 200, "{body}");
        let token = json_of(&body)["token"].as_str().unwrap().to_string();
        assert!(auth::is_token(&token));
        let bearer = format!("Authorization: Bearer {token}");
        assert_eq!(call(port, "GET", "/api/board", &[&bearer], "").await.0, 200);
        assert_eq!(call(port, "GET", "/api/board", &["Authorization: Bearer kz_nope"], "").await.0, 401);
        let (_, _, body) = call(port, "GET", "/api/tokens", &[&ana], "").await;
        assert_eq!(json_of(&body)[0]["name"], "Claude Code");
        assert!(json_of(&body)[0].get("token").is_none(), "shown once only");

        let say = json!({ "jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": {
            "name": "add_comment", "arguments": { "id": "api#1", "body": "done" },
            "_meta": { "io.modelcontextprotocol/protocolVersion": "2026-07-28",
                       "io.modelcontextprotocol/clientCapabilities": {},
                       "io.modelcontextprotocol/clientInfo": { "name": "claude-code", "version": "1" } } } });
        let mcp = [
            "Accept: application/json, text/event-stream",
            "MCP-Protocol-Version: 2026-07-28",
            "Mcp-Method: tools/call",
            "Mcp-Name: add_comment",
        ];
        assert_eq!(call(port, "POST", "/mcp", &mcp, &say.to_string()).await.0, 401, "no token, no board");
        let with_token: Vec<&str> = mcp.iter().copied().chain([bearer.as_str()]).collect();
        let (status, _, body) = call(port, "POST", "/mcp", &with_token, &say.to_string()).await;
        assert!(status == 200 && body.contains("Comment added"), "{status} {body}");
        let last = home.store().unwrap().list_comments(task.id).unwrap().pop().unwrap();
        assert_eq!((last.author.as_str(), last.user_id), ("claude-code", Some(1)));

        // Removing a user ends their sessions with them.
        assert_eq!(call(port, "DELETE", &ben_path, &[&ana], "").await.0, 200);
        assert_eq!(call(port, "GET", "/api/board", &[&ben], "").await.0, 401);
        assert_eq!(home.store().unwrap().get_task(task.id).unwrap().assignee, None);
    }

    #[tokio::test]
    async fn a_profile_picture_is_an_image_the_board_saved() {
        let tmp = tempfile::tempdir().unwrap();
        let home = Home::open(tmp.path().to_path_buf()).unwrap();
        let port = serve_board(&home, Net::Private).await;
        let (_, head, _) = call(port, "POST", "/api/users", &[], r#"{"name":"Ana","password":"correct horse"}"#).await;
        let ana = signed_in(&head);

        let missing = r#"{"avatar":"00000000000000aa.png"}"#;
        assert_eq!(call(port, "PATCH", "/api/users/1", &[&ana], missing).await.0, 400, "never uploaded");
        for file in ["00000000000000aa.png", "00000000000000bb.png"] {
            crate::home::write_whole(&home.images().join(file), b"png").unwrap();
        }
        let (status, _, body) = call(port, "PATCH", "/api/users/1", &[&ana], missing).await;
        assert_eq!(status, 200, "{body}");
        assert_eq!(json_of(&body)["avatar"], "00000000000000aa.png");
        assert_eq!(call(port, "GET", "/images/00000000000000aa.png", &[&ana], "").await.0, 200);

        // A new picture, or none, takes the old file with it.
        call(port, "PATCH", "/api/users/1", &[&ana], r#"{"avatar":"00000000000000bb.png"}"#).await;
        assert!(!home.images().join("00000000000000aa.png").exists());
        let (_, _, body) = call(port, "PATCH", "/api/users/1", &[&ana], r#"{"name":"Ana Lima"}"#).await;
        assert_eq!(json_of(&body)["avatar"], "00000000000000bb.png", "left out, so left alone");
        let (_, _, body) = call(port, "PATCH", "/api/users/1", &[&ana], r#"{"avatar":null}"#).await;
        assert!(json_of(&body).get("avatar").is_none());
        assert!(!home.images().join("00000000000000bb.png").exists());
    }

    #[tokio::test]
    async fn a_joined_board_signs_in_to_the_shared_one_with_its_token() {
        let (host_dir, guest_dir) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
        let host = Home::open(host_dir.path().to_path_buf()).unwrap();
        let ana = host.store().unwrap().create_user("Ana", "hash", false).unwrap();
        let token = auth::new_token();
        host.store().unwrap().create_token(ana.id, "laptop", &auth::digest(&token)).unwrap();
        let host_port = serve_board(&host, Net::Sharing).await;
        let address = format!("127.0.0.1:{host_port}");

        let guest = Home::open(guest_dir.path().to_path_buf()).unwrap();
        std::fs::write(guest.dir.join(share::FILE), format!("join {address}\n")).unwrap();
        let without = serve_board(&guest, Net::Joined(address.clone())).await;
        assert_eq!(call(without, "GET", "/remote/api/board", &[], "").await.0, 401, "the shared board has accounts");

        std::fs::write(guest.dir.join(share::FILE), format!("join {address} {token}\n")).unwrap();
        assert_eq!(Net::load(&guest), Net::Joined(address.clone()));
        assert_eq!(share::token(&guest).as_deref(), Some(token.as_str()));
        let guard = Guard::new(0, Ipv4Addr::LOCALHOST.into(), Net::Joined(address)).with_token(share::token(&guest));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let with = listener.local_addr().unwrap().port();
        let guard = Arc::new(Guard { port: with, ..guard });
        let app = router(guest.clone(), guard, CancellationToken::new());
        tokio::spawn(
            async move { axum::serve(listener, app.into_make_service_with_connect_info::<SocketAddr>()).await },
        );
        // Whatever the browser sent to this board stays here: the token is what signs in there.
        let (status, _, body) = call(with, "GET", "/remote/api/session", &["Cookie: kanzo_session=local"], "").await;
        assert_eq!(status, 200, "{body}");
        assert_eq!(json_of(&body)["user"]["name"], "Ana");
    }
}
