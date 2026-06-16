# iroh-http-gateway

HTTP/1.1 gateway for services exposed with
[`dumbpipe`](https://github.com/n0-computer/dumbpipe) over
[`iroh`](https://www.iroh.computer/).

Incoming requests are routed by the left-most DNS label:

```text
http://<base32-encoded-32-byte-endpoint-id>.example.com/
```

For each request, the gateway:

1. extracts the 52-character lowercase base32 iroh endpoint ID from `Host`,
2. dials that endpoint with `dumbpipe::ALPN`,
3. opens one bidirectional stream,
4. writes the dumbpipe handshake, and
5. forwards the HTTP/1.1 request and response over that stream.

## Usage

```sh
cargo run -- --listen 0.0.0.0:8080 --base-domain example.com
```

`--listen` also accepts Unix socket paths:

```sh
cargo run -- --listen /tmp/iroh-http-gateway.sock --base-domain example.com
```

Run the remote node with dumbpipe, forwarding to an HTTP service:

```sh
dumbpipe listen-tcp --host 127.0.0.1:3000
```

Configure DNS so `*.example.com` points to the gateway. Then request:

```sh
curl http://<endpoint-id>.example.com/
```

## Ticket Translation API

Dumbpipe prints endpoint tickets by default. To expose a gateway-local API that
turns those tickets into hostname-safe endpoint IDs, configure an API hostname:

```sh
cargo run -- \
  --listen 0.0.0.0:8080 \
  --base-domain example.com \
  --api-hostname api.example.com
```

Requests to `--api-hostname` take precedence over gateway routing. `/info`
returns the gateway node's own endpoint ID as JSON:

```sh
curl 'http://api.example.com/info'
```

The translate endpoint returns a lowercase unpadded RFC4648 base32 endpoint ID
as `text/plain`:

```sh
curl 'http://api.example.com/translate?ticket=<dumbpipe-ticket>'
```

Set `IROH_SECRET` or pass `--iroh-secret` with a hex-encoded iroh secret key to
make the gateway's iroh endpoint stable across restarts. Without one, an
ephemeral key is generated and emitted in the startup logs.

Logs are emitted with the `tracing` crate as JSON Lines. On startup, the gateway
logs its own iroh endpoint ID as `endpoint_id`. Set `RUST_LOG` to tune verbosity:

```sh
RUST_LOG=iroh_http_gateway=debug,iroh=info cargo run -- --base-domain example.com
```

Useful options:

```text
--listen <ADDR|PATH>           HTTP TCP listen address or Unix socket path, default 0.0.0.0:8080
--base-domain <DOMAIN>        require hosts to match <endpoint-id>.<DOMAIN>
--api-hostname <HOSTNAME>     serve local API routes on this exact hostname
--iroh-ipv4-addr <ADDR>       iroh IPv4 bind address
--iroh-ipv6-addr <ADDR>       iroh IPv6 bind address
--online-timeout-ms <MS>      startup wait for iroh online status
```
