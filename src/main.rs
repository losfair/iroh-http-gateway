mod iroh_stream;

use std::{
    convert::Infallible,
    fmt,
    net::{SocketAddr, SocketAddrV4, SocketAddrV6},
    path::{Path, PathBuf},
    str::FromStr,
    sync::Arc,
    time::Duration,
};

use anyhow::Context as _;
use bytes::Bytes;
use clap::Parser;
use http::{HeaderValue, StatusCode};
use http_body_util::{BodyExt, Full, combinators::BoxBody};
use hyper::{
    Method, Request, Response, body::Incoming, client::conn::http1 as client_http1,
    server::conn::http1 as server_http1, service::service_fn,
};
use hyper_util::rt::TokioIo;
use iroh::{Endpoint, EndpointAddr, EndpointId, SecretKey, endpoint::presets};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::{TcpListener, UnixListener},
    time::timeout,
};
use tracing::{debug, info, warn};

use crate::iroh_stream::IrohStream;

type GatewayBody = BoxBody<Bytes, hyper::Error>;

#[derive(Parser, Debug)]
#[command(about = "HTTP/1.1 gateway for dumbpipe services addressed by iroh endpoint ID")]
struct Args {
    /// HTTP listen address.
    ///
    /// Values that parse as socket addresses bind TCP. Other values are treated
    /// as Unix socket paths. Unix paths may optionally be prefixed with unix:.
    #[arg(long, default_value = "0.0.0.0:8080")]
    listen: ListenAddr,

    /// Optional base domain to require after the endpoint-id label, e.g. example.com.
    #[arg(long)]
    base_domain: Option<String>,

    /// Optional hostname for gateway-local API routes.
    ///
    /// When configured, requests for this exact hostname are handled before
    /// endpoint-id gateway routing. Currently serves /translate?ticket=...
    #[arg(long)]
    api_hostname: Option<String>,

    /// Iroh IPv4 UDP bind address. Random port/interface by default.
    #[arg(long)]
    iroh_ipv4_addr: Option<SocketAddrV4>,

    /// Iroh IPv6 UDP bind address. Random port/interface by default.
    #[arg(long)]
    iroh_ipv6_addr: Option<SocketAddrV6>,

    /// Hex-encoded iroh secret key. Defaults to IROH_SECRET, then a generated key.
    #[arg(long, env = "IROH_SECRET")]
    iroh_secret: Option<String>,

    /// Milliseconds to wait for the local iroh endpoint to come online at startup.
    #[arg(long, default_value_t = 10_000)]
    online_timeout_ms: u64,
}

#[derive(Clone)]
struct Gateway {
    endpoint: Endpoint,
    endpoint_id: String,
    base_domain: Option<String>,
    api_hostname: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum ListenAddr {
    Tcp(SocketAddr),
    Unix(PathBuf),
}

impl FromStr for ListenAddr {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if let Ok(addr) = value.parse::<SocketAddr>() {
            return Ok(Self::Tcp(addr));
        }

        let path = value.strip_prefix("unix:").unwrap_or(value);
        if path.is_empty() {
            return Err("listen value must be a TCP socket address or Unix socket path".to_owned());
        }

        Ok(Self::Unix(PathBuf::from(path)))
    }
}

impl fmt::Display for ListenAddr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Tcp(addr) => write!(f, "{addr}"),
            Self::Unix(path) => write!(f, "unix:{}", path.display()),
        }
    }
}

enum Listener {
    Tcp(TcpListener),
    Unix(UnixListener),
}

impl Listener {
    async fn bind(addr: &ListenAddr) -> std::io::Result<Self> {
        match addr {
            ListenAddr::Tcp(addr) => TcpListener::bind(addr).await.map(Self::Tcp),
            ListenAddr::Unix(path) => UnixListener::bind(path).map(Self::Unix),
        }
    }

    fn local_addr(&self) -> String {
        match self {
            Self::Tcp(listener) => match listener.local_addr() {
                Ok(addr) => addr.to_string(),
                Err(err) => format!("<unknown tcp address: {err}>"),
            },
            Self::Unix(listener) => match listener.local_addr() {
                Ok(addr) => unix_socket_addr_display(&addr),
                Err(err) => format!("<unknown unix socket address: {err}>"),
            },
        }
    }
}

#[derive(Debug)]
struct GatewayError {
    status: StatusCode,
    message: String,
}

impl GatewayError {
    fn bad_request(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            message: message.into(),
        }
    }

    fn bad_gateway(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_GATEWAY,
            message: message.into(),
        }
    }
}

impl fmt::Display for GatewayError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for GatewayError {}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .json()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let args = Args::parse();
    let secret_key = get_or_create_secret(args.iroh_secret.as_deref())?;

    let mut builder = Endpoint::builder(presets::N0).secret_key(secret_key);
    if let Some(addr) = args.iroh_ipv4_addr {
        builder = builder.bind_addr(SocketAddr::V4(addr))?;
    }
    if let Some(addr) = args.iroh_ipv6_addr {
        builder = builder.bind_addr(SocketAddr::V6(addr))?;
    }

    let endpoint = builder.bind().await?;
    let own_endpoint_id = endpoint.id();
    let own_endpoint_id_z32 = endpoint_id_to_z32(&own_endpoint_id);
    info!(
        endpoint_id = %own_endpoint_id_z32,
        endpoint_id_hex = %own_endpoint_id,
        "local iroh endpoint bound"
    );

    if timeout(
        Duration::from_millis(args.online_timeout_ms),
        endpoint.online(),
    )
    .await
    .is_err()
    {
        warn!("local iroh endpoint did not report online before timeout");
    }

    let gateway = Arc::new(Gateway {
        endpoint,
        endpoint_id: own_endpoint_id_z32,
        base_domain: args.base_domain.as_deref().map(normalize_domain),
        api_hostname: args.api_hostname.as_deref().map(normalize_domain),
    });

    let listener = Listener::bind(&args.listen).await?;
    let local_addr = listener.local_addr();
    info!(listen = %local_addr, "listening for HTTP/1.1 requests");

    loop {
        match &listener {
            Listener::Tcp(listener) => {
                let (stream, remote_addr) = listener.accept().await?;
                spawn_http_connection(stream, remote_addr.to_string(), Arc::clone(&gateway));
            }
            Listener::Unix(listener) => {
                let (stream, remote_addr) = listener.accept().await?;
                spawn_http_connection(
                    stream,
                    unix_socket_addr_display(&remote_addr),
                    Arc::clone(&gateway),
                );
            }
        }
    }
}

fn spawn_http_connection<S>(stream: S, remote_addr: String, gateway: Arc<Gateway>)
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    tokio::spawn(async move {
        debug!(%remote_addr, "accepted HTTP connection");
        let service = service_fn(move |req| {
            let gateway = Arc::clone(&gateway);
            async move { Ok::<_, Infallible>(gateway.serve(req).await) }
        });

        if let Err(err) = server_http1::Builder::new()
            .preserve_header_case(true)
            .title_case_headers(true)
            .serve_connection(TokioIo::new(stream), service)
            .await
        {
            warn!(%remote_addr, error = %err, "HTTP connection failed");
        }
    });
}

fn unix_socket_addr_display(addr: &tokio::net::unix::SocketAddr) -> String {
    addr.as_pathname()
        .map(Path::display)
        .map(|display| format!("unix:{display}"))
        .unwrap_or_else(|| "<unnamed unix socket>".to_owned())
}

impl Gateway {
    async fn serve(&self, req: Request<Incoming>) -> Response<GatewayBody> {
        if self.api_hostname.is_some() {
            match self.is_api_request(&req) {
                Ok(true) => return self.serve_api(&req),
                Ok(false) => {}
                Err(err) => return error_response(err.status, err.message),
            }
        }

        match self.proxy(req).await {
            Ok(resp) => resp,
            Err(err) => {
                if err.status.is_server_error() {
                    warn!(status = %err.status, error = %err, "proxy failed");
                }
                error_response(err.status, err.message)
            }
        }
    }

    async fn proxy(&self, req: Request<Incoming>) -> Result<Response<GatewayBody>, GatewayError> {
        let endpoint_id = endpoint_id_from_request(&req, self.base_domain.as_deref())?;
        let endpoint_addr = EndpointAddr::new(endpoint_id);

        debug!(%endpoint_id, "dialing dumbpipe endpoint");
        let conn = self
            .endpoint
            .connect(endpoint_addr, dumbpipe::ALPN)
            .await
            .map_err(|err| GatewayError::bad_gateway(format!("failed to dial endpoint: {err}")))?;

        let (mut send, recv) = conn
            .open_bi()
            .await
            .map_err(|err| GatewayError::bad_gateway(format!("failed to open stream: {err}")))?;
        send.write_all(&dumbpipe::HANDSHAKE).await.map_err(|err| {
            GatewayError::bad_gateway(format!("failed to write handshake: {err}"))
        })?;

        let io = TokioIo::new(IrohStream { send, recv });
        let (mut sender, connection) = client_http1::Builder::new()
            .preserve_header_case(true)
            .title_case_headers(true)
            .handshake(io)
            .await
            .map_err(|err| {
                GatewayError::bad_gateway(format!("failed to start upstream HTTP/1.1: {err}"))
            })?;

        tokio::spawn(async move {
            if let Err(err) = connection.await {
                warn!(error = %err, "upstream HTTP/1.1 connection failed");
            }
        });

        sender
            .send_request(req)
            .await
            .map(|resp| resp.map(|body| body.boxed()))
            .map_err(|err| GatewayError::bad_gateway(format!("upstream request failed: {err}")))
    }

    fn is_api_request(&self, req: &Request<Incoming>) -> Result<bool, GatewayError> {
        let Some(api_hostname) = self.api_hostname.as_deref() else {
            return Ok(false);
        };
        let host = request_host(req)?;
        Ok(normalize_host(host) == api_hostname)
    }

    fn serve_api(&self, req: &Request<Incoming>) -> Response<GatewayBody> {
        match api_request(req, &self.endpoint_id) {
            Ok(ApiResponse::Json(json)) => response_with_content_type(
                StatusCode::OK,
                json,
                HeaderValue::from_static("application/json"),
            ),
            Ok(ApiResponse::Text(text)) => response_with_content_type(
                StatusCode::OK,
                text,
                HeaderValue::from_static("text/plain; charset=utf-8"),
            ),
            Err(err) => error_response(err.status, err.message),
        }
    }
}

fn get_or_create_secret(secret: Option<&str>) -> anyhow::Result<SecretKey> {
    match secret {
        Some(secret) => SecretKey::from_str(secret).context("invalid iroh secret key"),
        None => {
            let key = SecretKey::generate();
            warn!(
                iroh_secret = %hex::encode(key.to_bytes()),
                "generated ephemeral iroh secret key"
            );
            Ok(key)
        }
    }
}

fn endpoint_id_from_request(
    req: &Request<Incoming>,
    base_domain: Option<&str>,
) -> Result<EndpointId, GatewayError> {
    let host = request_host(req)?;
    let label = endpoint_label_from_host(host, base_domain)?;
    parse_endpoint_id_label(&label)
}

fn request_host<B>(req: &Request<B>) -> Result<&str, GatewayError> {
    let host = req
        .headers()
        .get(http::header::HOST)
        .ok_or_else(|| GatewayError::bad_request("missing Host header"))?;
    host_to_str(host)
}

fn host_to_str(host: &HeaderValue) -> Result<&str, GatewayError> {
    host.to_str()
        .map_err(|_| GatewayError::bad_request("Host header is not valid ASCII"))
}

fn endpoint_label_from_host(host: &str, base_domain: Option<&str>) -> Result<String, GatewayError> {
    let host = normalize_host(host);
    if host.is_empty() {
        return Err(GatewayError::bad_request("Host header is empty"));
    }

    match base_domain {
        Some(base_domain) => {
            let suffix = format!(".{base_domain}");
            let Some(label) = host.strip_suffix(&suffix) else {
                return Err(GatewayError::bad_request(format!(
                    "Host must end with .{base_domain}"
                )));
            };
            if label.contains('.') || label.is_empty() {
                return Err(GatewayError::bad_request(
                    "Host must be <z32-endpoint-id>.<base-domain>",
                ));
            }
            Ok(label.to_owned())
        }
        None => host
            .split('.')
            .next()
            .filter(|label| !label.is_empty())
            .map(str::to_owned)
            .ok_or_else(|| GatewayError::bad_request("Host is missing endpoint-id label")),
    }
}

fn strip_host_port(host: &str) -> &str {
    if let Some(host) = host.strip_prefix('[') {
        return host.split(']').next().unwrap_or(host);
    }

    match host.rsplit_once(':') {
        Some((name, port)) if port.bytes().all(|b| b.is_ascii_digit()) => name,
        _ => host,
    }
}

fn normalize_host(host: &str) -> String {
    strip_host_port(host)
        .trim_end_matches('.')
        .to_ascii_lowercase()
}

fn parse_endpoint_id_label(label: &str) -> Result<EndpointId, GatewayError> {
    if label.len() != 52 {
        return Err(GatewayError::bad_request(
            "endpoint-id label must be 52 z32 characters",
        ));
    }

    if !label.bytes().all(is_z32_char) {
        return Err(GatewayError::bad_request(
            "endpoint-id label must use lowercase z-base-32 characters",
        ));
    }

    EndpointId::from_z32(label)
        .map_err(|err| GatewayError::bad_request(format!("invalid endpoint id: {err}")))
}

fn is_z32_char(ch: u8) -> bool {
    matches!(
        ch,
        b'y' | b'b'
            | b'n'
            | b'd'
            | b'r'
            | b'f'
            | b'g'
            | b'8'
            | b'e'
            | b'j'
            | b'k'
            | b'm'
            | b'c'
            | b'p'
            | b'q'
            | b'x'
            | b'o'
            | b't'
            | b'1'
            | b'u'
            | b'w'
            | b'i'
            | b's'
            | b'z'
            | b'a'
            | b'3'
            | b'4'
            | b'5'
            | b'h'
            | b'7'
            | b'6'
            | b'9'
    )
}

enum ApiResponse {
    Json(String),
    Text(String),
}

fn api_request<B>(req: &Request<B>, endpoint_id: &str) -> Result<ApiResponse, GatewayError> {
    if req.method() != Method::GET {
        return Err(GatewayError {
            status: StatusCode::METHOD_NOT_ALLOWED,
            message: "method not allowed".to_owned(),
        });
    }

    match req.uri().path() {
        "/info" => Ok(ApiResponse::Json(format!(
            "{{\"endpoint_id\":\"{endpoint_id}\"}}\n"
        ))),
        "/translate" => translate_ticket_request(req).map(ApiResponse::Text),
        _ => Err(GatewayError {
            status: StatusCode::NOT_FOUND,
            message: "not found".to_owned(),
        }),
    }
}

fn translate_ticket_request<B>(req: &Request<B>) -> Result<String, GatewayError> {
    let ticket = req
        .uri()
        .query()
        .and_then(|query| {
            url::form_urlencoded::parse(query.as_bytes())
                .find(|(key, _)| key == "ticket")
                .map(|(_, value)| value.into_owned())
        })
        .ok_or_else(|| GatewayError::bad_request("missing ticket query parameter"))?;

    let ticket = dumbpipe::EndpointTicket::from_str(&ticket)
        .map_err(|err| GatewayError::bad_request(format!("invalid ticket: {err}")))?;
    Ok(endpoint_id_to_z32(&ticket.endpoint_addr().id))
}

fn endpoint_id_to_z32(endpoint_id: &EndpointId) -> String {
    endpoint_id.to_z32()
}

fn normalize_domain(domain: &str) -> String {
    domain.trim().trim_end_matches('.').to_ascii_lowercase()
}

fn error_response(status: StatusCode, message: impl Into<String>) -> Response<GatewayBody> {
    let mut resp = Response::new(full(message.into()));
    *resp.status_mut() = status;
    resp
}

fn response_with_content_type(
    status: StatusCode,
    body: impl Into<String>,
    content_type: HeaderValue,
) -> Response<GatewayBody> {
    let mut resp = Response::new(full(body.into()));
    *resp.status_mut() = status;
    resp.headers_mut()
        .insert(http::header::CONTENT_TYPE, content_type);
    resp
}

fn full(chunk: impl Into<Bytes>) -> GatewayBody {
    Full::new(chunk.into())
        .map_err(|never| match never {})
        .boxed()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_endpoint_label_from_host_with_base_domain() {
        let key = SecretKey::generate();
        let label = endpoint_id_to_z32(&key.public());
        let host = format!("{label}.example.com:8080");

        assert_eq!(
            endpoint_label_from_host(&host, Some("example.com"))
                .unwrap()
                .as_str(),
            label.as_str()
        );
    }

    #[test]
    fn rejects_extra_subdomain_when_base_domain_is_configured() {
        let key = SecretKey::generate();
        let label = endpoint_id_to_z32(&key.public());
        let host = format!("extra.{label}.example.com");

        assert!(endpoint_label_from_host(&host, Some("example.com")).is_err());
    }

    #[test]
    fn rejects_non_z32_endpoint_label() {
        let label = "hwpbkwcfcubxe4fwu5u5eobrsbwyfwiokk5qahza3edvwuqqfbm0";

        assert!(parse_endpoint_id_label(label).is_err());
    }

    #[test]
    fn accepts_uppercase_host_label() {
        let key = SecretKey::generate();
        let label = endpoint_id_to_z32(&key.public());
        let host = format!("{}.EXAMPLE.COM", label.to_ascii_uppercase());

        let parsed_label = endpoint_label_from_host(&host, Some("example.com")).unwrap();

        assert_eq!(parsed_label, label);
        parse_endpoint_id_label(&parsed_label).unwrap();
    }

    #[test]
    fn accepts_generated_endpoint_id_display() {
        let key = SecretKey::generate();
        let label = endpoint_id_to_z32(&key.public());

        parse_endpoint_id_label(&label).unwrap();
    }

    #[test]
    fn translates_dumbpipe_ticket_to_z32_endpoint_id() {
        let key = SecretKey::generate();
        let endpoint_id = key.public();
        let ticket = dumbpipe::EndpointTicket::new(EndpointAddr::new(endpoint_id));
        let uri = format!("/translate?ticket={ticket}");
        let req = Request::builder()
            .method(Method::GET)
            .uri(uri)
            .body(())
            .unwrap();

        assert_eq!(
            translate_ticket_request(&req).unwrap(),
            endpoint_id_to_z32(&endpoint_id)
        );
    }

    #[test]
    fn info_api_returns_endpoint_id_json() {
        let key = SecretKey::generate();
        let endpoint_id = endpoint_id_to_z32(&key.public());
        let req = Request::builder()
            .method(Method::GET)
            .uri("/info")
            .body(())
            .unwrap();

        match api_request(&req, &endpoint_id).unwrap() {
            ApiResponse::Json(json) => {
                assert_eq!(json, format!("{{\"endpoint_id\":\"{endpoint_id}\"}}\n"));
            }
            ApiResponse::Text(_) => panic!("expected JSON response"),
        }
    }

    #[test]
    fn parses_tcp_listen_addr() {
        assert_eq!(
            "127.0.0.1:8080".parse::<ListenAddr>().unwrap(),
            ListenAddr::Tcp("127.0.0.1:8080".parse().unwrap())
        );
    }

    #[test]
    fn parses_unix_listen_addr() {
        assert_eq!(
            "/tmp/iroh-http-gateway.sock".parse::<ListenAddr>().unwrap(),
            ListenAddr::Unix(PathBuf::from("/tmp/iroh-http-gateway.sock"))
        );
        assert_eq!(
            "unix:/tmp/iroh-http-gateway.sock"
                .parse::<ListenAddr>()
                .unwrap(),
            ListenAddr::Unix(PathBuf::from("/tmp/iroh-http-gateway.sock"))
        );
    }
}
