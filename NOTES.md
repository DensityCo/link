# link - Implementation Notes

## Plan

- [x] Look through the nerves_hub_link project and build CLIENT.md
- [x] mTLS websocket connection
- [x] Alternate Shared Secret connection
- [x] Firmware URL delivery over the WebSocket
- [x] Support static config identity and runtime-provided device metadata
- [x] Configuration to select connection method, server URI and other config
- [x] Basic firmware metadata reporting for device
- [x] Basic health extension reporting
- [x] Apply firmware through a pluggable installer
- [x] Run as daemon

## Architecture

```
src/
  main.rs          - Entry point, daemon mode with reconnection
  lib.rs           - Library entrypoint
  config.rs        - Configuration (TOML file parsing)
  device.rs        - Platform-agnostic device metadata and provider trait
  extensions/
    mod.rs
    health.rs      - Health extension report trait and system reporter
  runner.rs        - Reconnect loop with exponential backoff
  transport.rs     - WebSocket/TLS connection setup
  client_update.rs - Client-side update protocol/status handling
  auth/
    mod.rs         - Auth module
    mtls.rs        - mTLS TLS config builder (cert/key/CA loading)
    shared_secret.rs - Shared Secret HMAC auth (PBKDF2 + HMAC-SHA256)
  protocol/
    mod.rs
    channel.rs     - Phoenix Channels wire message framing
    events.rs      - Typed protocol event names
    status.rs      - NervesHub status/progress payload builders
  update/
    mod.rs
    types.rs       - Firmware update payload types
    download.rs    - Firmware download and progress calculation
    fwup.rs        - fwup command execution
    installer.rs   - FirmwareInstaller trait and FwupInstaller
    runner.rs      - Download/apply orchestration with typed progress events
  client.rs        - Link client facade and channel event loop
```

## Dependencies

- tokio: async runtime
- tokio-tungstenite: WebSocket client
- rustls + rustls-pemfile + tokio-rustls: TLS with mTLS support
- serde + serde_json: JSON serialization
- reqwest: HTTPS firmware download with streaming
- hmac + sha2 + pbkdf2: Shared Secret crypto
- base64: encoding
- toml: config file parsing
- tracing + tracing-subscriber: structured logging
- thiserror: error types
- rand: jitter for backoff

## Tests

- config: TOML parsing, validation, defaults, both auth types
- protocol/channel: message parsing, building (join/heartbeat/push), roundtrip, error cases
- protocol/events: typed event name mapping
- protocol/status: status and progress payload serialization
- shared_secret: algorithm string, header generation, determinism, differentiation
- mtls: file loading error cases (missing, empty)
- extensions/health: extension selection parsing, basic system metrics
- update: update message parsing, progress calculation, safe firmware paths
- client: creation, join payload with metadata, runtime device info
- runner: backoff delay behavior

Local test execution requires a Rust toolchain (`cargo`) to be installed.

## Notes

- Shared Secret uses `/device-socket/websocket`; mTLS uses `/socket/websocket`
- Device identity and firmware metadata come from static config, direct `DeviceInfo`, or `DeviceInfoProvider`; callers can update runtime values with `LinkClient::set_device_info`
- The library does not read Nerves KV or call Nerves runtime APIs
- Phoenix Channels messages are JSON arrays: [join_ref, ref, topic, event, payload]
- Heartbeat interval: 30 seconds (configurable)
- Reconnect with exponential backoff: 1s -> 60s with 50% jitter
- Shared Secret signature has 90 second validity window
- Firmware downloads are full (no resume), written through a temp file and applied via a pluggable installer
- Download progress is reported every 5% increment with `stage: "downloading"`
