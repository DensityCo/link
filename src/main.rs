use link::{ClientEvent, Config, LinkClient, LinkRunner};
use std::path::PathBuf;
use tokio::sync::mpsc;
use tracing::{error, info, warn};

async fn run_daemon(config: Config) -> Result<(), Box<dyn std::error::Error>> {
    let device_info_provider = config.device_info_provider();
    let client = match device_info_provider {
        Some(provider) => LinkClient::from_provider(config, provider)?,
        None => LinkClient::new(config)?,
    };
    let runner = LinkRunner::new(client);
    let (event_tx, mut event_rx) = mpsc::channel::<ClientEvent>(32);

    let event_handle = tokio::spawn(async move {
        while let Some(event) = event_rx.recv().await {
            match event {
                ClientEvent::Connected => info!("connected to server"),
                ClientEvent::Joined => info!("joined device channel"),
                ClientEvent::ExtensionsJoined => info!("joined extensions channel"),
                ClientEvent::ConsoleJoined => info!("joined console channel"),
                ClientEvent::ConsoleStarted => info!("console session started"),
                ClientEvent::ConsoleStopped => info!("console session stopped"),
                ClientEvent::HealthReported => info!("health report sent"),
                ClientEvent::DeploymentAvailable(deployment) => {
                    info!(
                        uuid = %deployment.firmware_meta.uuid,
                        version = %deployment.firmware_meta.version,
                        "deployment available"
                    );
                }
                ClientEvent::FirmwareDownloaded(path) => {
                    info!(path = %path.display(), "firmware downloaded");
                }
                ClientEvent::FirmwareApplied => {
                    info!("firmware applied successfully");
                }
                ClientEvent::RebootRequested => {
                    info!("reboot requested by server");
                }
                ClientEvent::IdentifyRequested => {
                    info!("identify requested by server");
                }
                ClientEvent::ScriptRequested(script_ref) => {
                    info!(script_ref = %script_ref, "script requested by server");
                }
                ClientEvent::ScriptCompleted(script_ref) => {
                    info!(script_ref = %script_ref, "script completed");
                }
                ClientEvent::ScriptFailed { script_ref, reason } => {
                    warn!(script_ref = %script_ref, reason = %reason, "script failed");
                }
                ClientEvent::Disconnected(reason) => {
                    warn!(reason = %reason, "disconnected");
                }
            }
        }
    });

    let result = runner.run(event_tx).await;
    event_handle.abort();
    result.map_err(|error| Box::new(error) as Box<dyn std::error::Error>)
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let config_path = std::env::args()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("/etc/link/config.toml"));

    let config = match Config::from_file(&config_path) {
        Ok(c) => c,
        Err(e) => {
            error!(path = %config_path.display(), error = %e, "failed to load config");
            std::process::exit(1);
        }
    };

    info!(
        host = %config.host,
        "starting link daemon"
    );

    if let Err(e) = run_daemon(config).await {
        error!(error = %e, "daemon failed");
        std::process::exit(1);
    }
}
