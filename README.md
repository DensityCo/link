# link

A Rust library and daemon for Fabric Fleet's device deployment protocol. It connects over WebSocket/Phoenix Channels, receives deployment requests, downloads firmware, and applies it through a configurable installer. The default daemon installer uses `fwup`.

The library is intentionally platform agnostic. It does not read Nerves KV, call `Nerves.Runtime`, or know how a device stores its identity. Callers provide serial number, firmware metadata, and runtime state through config, direct `DeviceInfo`, or the `DeviceInfoProvider` trait.

## Building

```
cargo build --release
```

The resulting binary is at `target/release/link`.

## Running

```
link /path/to/config.toml
```

If no path is given, it defaults to `/etc/link/config.toml`.

Logging is controlled via the `RUST_LOG` environment variable:

```
RUST_LOG=debug link config.toml
```

## Configuration

Configuration is a TOML file. See `examples/` for complete samples.

### Common fields

| Field | Required | Default | Description |
|-------|----------|---------|-------------|
| `host` | yes | | Server base URL (e.g. `https://fleet.fabric.density.ai/`) |
| `serial_number` | * | | Static device serial number |
| `fwup_devpath` | no | `/dev/mmcblk0` | Block device for fwup to write to |
| `fwup_task` | no | `upgrade` | fwup task name |
| `fwup_public_keys` | no | `[]` | Firmware signing public keys passed to `fwup --public-key` |
| `heartbeat_interval_secs` | no | `30` | Seconds between heartbeats |
| `data_dir` | no | `/tmp/link` | Directory for temporary firmware downloads |
| `device_api_version` | no | `2.3.0` | API version reported to the server |
| `console_version` | no | | Legacy top-level console API version; prefer `[console].version` |
| `fwup_version` | no | | Current `fwup` version reported to the server |

\* The daemon constructor needs `serial_number` when it builds device metadata from config. Library callers can omit it and provide identity plus device settings through `LinkClient::with_device_info` or `LinkClient::from_provider`.

### Firmware metadata

For the config-backed daemon path, the `[firmware]` section describes the currently running firmware:

```toml
[firmware]
uuid = "aaaa-bbbb-cccc"
version = "1.0.0"
platform = "rpi4"
architecture = "arm"
product = "my-product"
```

All fields in this section are required when the daemon builds `DeviceInfo` from config. Library callers using `LinkClient::with_device_info` or `LinkClient::from_provider` can omit this section and supply firmware metadata directly.

### Authentication

Two methods are supported. Shared Secret connects to `/device-socket/websocket`; mTLS connects to `/socket/websocket`. `http` and `https` base URLs are converted to `ws` and `wss` for the socket connection.

#### Shared Secret

```toml
[auth]
type = "shared_secret"
key = "device-key-identifier"
secret = "the-shared-secret"
```

### Console

The device console is disabled by default. Enabling it grants remote shell access through the server console UI, so only enable it for devices and products where that is intended.

```toml
[console]
enabled = true
version = "2.0.0"
command = "/bin/sh"
args = []
timeout_secs = 300
rows = 24
cols = 80
```

The default daemon backend starts the configured command in a PTY, forwards server `dn` input to stdin, sends PTY output as `up`, handles `window_size`, restarts the shell on `restart`, and writes console file uploads under `data_dir`. Library callers can provide their own backend with `LinkClient::set_console_backend`.

The `key` is the identifier registered with Fabric Fleet. The `secret` is the corresponding shared secret. These are used to generate HMAC-signed headers on each connection.

### Identify

Device identification is disabled by default. Enable it when the target should run a command after the server sends an `identify` event, for example to blink LEDs or show a local indicator:

```toml
[identify]
enabled = true
command = "/usr/bin/identify-device"
args = ["--blink"]
```

Library callers can provide their own action with `LinkClient::set_identify_action`. Unlike reboot, the `identify` protocol event does not send a status message back to the server.

### Support Scripts

Support scripts are disabled by default. Enabling them allows the server to send `scripts/run` requests, so treat this as remote code execution and enable it only for devices and products where that is intended.

```toml
[scripts]
enabled = true
command = "/bin/sh"
args = ["-s"]
timeout_secs = 10
```

The default daemon command runner passes the script text to the configured command on stdin, captures stdout and stderr, enforces the timeout, and reports the result back on the device channel as `scripts/run`. Library callers can provide their own implementation with `LinkClient::set_script_runner`, which keeps the core library independent of Elixir, Nerves, or any particular script language.

### Reboot

Reboot execution is disabled by default so the same library can be used safely on development hosts and non-Nerves systems. Enable it in daemon config when the target should reboot after a successful firmware apply or when the server sends a reboot command:

```toml
[reboot]
enabled = true
command = "reboot"
args = []
after_firmware_apply = true
on_server_request = true
```

When reboot is enabled, the daemon sends the protocol `rebooting` status before executing the command. Library callers can provide their own reboot implementation with `LinkClient::set_rebooter`.

#### mTLS

```toml
[auth]
type = "mtls"
cert_path = "/etc/link/device-cert.pem"
key_path = "/etc/link/device-key.pem"
ca_cert_path = "/etc/link/ca.pem"
```

The device presents its client certificate during the TLS handshake. The server validates it against the CA chain.

### Device identity

The serial number identifies the device to the server. For config-backed daemon usage, set it directly:

```toml
serial_number = "device-001"
```

Applications embedding the library can omit `serial_number` from config and pass the serial number, firmware metadata, and runtime state through `LinkClient::with_device_info` or `LinkClient::from_provider`. If those values change while the process is running, update them with `LinkClient::set_device_info` before the next connection attempt.

### Firmware installer

The default daemon path builds a `FwupInstaller` from `fwup_devpath`, `fwup_task`, and `fwup_public_keys`. Configure at least one `fwup_public_keys` entry to have `fwup` verify firmware signatures before applying updates:

```toml
fwup_public_keys = [
  "REPLACE_WITH_FWUP_PUBLIC_KEY"
]
```

Library callers can provide their own installer by constructing a `DeploymentManager` with a custom `FirmwareInstaller` and setting it on the client:

```rust
let deployment_manager = DeploymentManager::with_installer(options, installer);
client.set_deployment_manager(deployment_manager);
```

### Health reports

`link` advertises the `health` extension when the server requests extensions. The server must select the extension in the extensions channel join response before reports are sent. In Fabric Fleet/NervesHub this means the health extension must be enabled for both the product and the device.

The default `SystemHealthReporter` reports platform-agnostic host metrics using the conventional `nerves_hub_link` metric keys for memory, CPU, load average, and disk usage: `cpu_usage_percent`, `mem_size_mb`, `mem_used_mb`, `mem_used_percent`, `load_1min`, `load_5min`, `load_15min`, `disk_total_kb`, `disk_available_kb`, and `disk_used_percentage`. Library callers can replace it with their own reporter:

```rust
client.set_health_reporter(reporter);
```

### Alarms

Alarms are reported through the health extension as `alarms` in the `health:report` payload. They are local runtime state and are intentionally Nerves-agnostic.

Library callers can set and clear alarms through the client's alarm store:

```rust
let alarms = client.alarm_store();

alarms.set("my_app.sensor_unavailable", "sensor process is not responding");
let alarm = alarms.get("my_app.sensor_unavailable");
let active_alarms = alarms.list();
alarms.clear("my_app.sensor_unavailable");
```

The client also manages these internal alarms:

| Alarm | Meaning |
|-------|---------|
| `link.disconnected` | The socket is disconnected or the last connection attempt failed |
| `link.update_in_progress` | A firmware update is currently running |
| `link.firmware_reverted` | Runtime device state reported firmware auto-revert detection |

## Behavior

On startup, `link`:

1. Reads the config file
2. Builds device metadata from static config
3. Connects to the server via WebSocket
4. Joins the device channel with caller-provided firmware metadata
5. Joins the extensions channel when requested and reports health if the server selects the `health` extension
6. Joins the console channel when `[console].enabled = true`
7. Sends heartbeats every 30 seconds
8. Runs the configured identify action when the server sends `identify`
9. Runs support scripts when `[scripts].enabled = true` and the server sends `scripts/run`
10. Includes current alarms in health reports
11. Listens for wire-level `update` events containing a firmware URL
12. Downloads the firmware to `data_dir` as `firmware-{uuid}.fw.tmp`, then renames it to `firmware-{uuid}.fw`
13. Applies it through the configured installer
14. Reports staged progress and completion to the server
15. Sends `rebooting` and executes the configured reboot hook when reboot is enabled

On disconnect, `LinkRunner` reconnects with exponential backoff (1s to 60s with jitter).

## Requirements

- The default daemon installer requires `fwup` on `PATH`
- The default console backend requires the configured shell command on `PATH`
- Identify support requires enabling `[identify]` and providing a command available on `PATH`
- Reboot support requires enabling `[reboot]` and providing a command available on `PATH`
- Support script execution requires enabling `[scripts]` and providing a command available on `PATH`
