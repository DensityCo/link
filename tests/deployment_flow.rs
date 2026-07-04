use futures_util::future::BoxFuture;
use futures_util::{SinkExt, StreamExt};
use link::deployment::{
    Deployment, DeploymentError, DeploymentManager, DeploymentOptions, FirmwareInstaller,
};
use link::device::FirmwareMetadata;
use link::protocol::Message;
use link::{AuthConfig, ClientEvent, Config, LinkClient};
use serde_json::{json, Value};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio::time::{timeout, Duration};

const FIRMWARE_BODY: &[u8] = b"test firmware bytes";
const TEST_UUID: &str = "integration-fw-uuid";

#[derive(Debug, Clone)]
struct InstallRecord {
    firmware_path: PathBuf,
    firmware_bytes: Vec<u8>,
    deployment_uuid: String,
}

#[derive(Clone)]
struct RecordingInstaller {
    installs: Arc<Mutex<Vec<InstallRecord>>>,
    failure: Option<String>,
}

impl RecordingInstaller {
    fn successful(installs: Arc<Mutex<Vec<InstallRecord>>>) -> Self {
        Self {
            installs,
            failure: None,
        }
    }

    fn failing(installs: Arc<Mutex<Vec<InstallRecord>>>, reason: impl Into<String>) -> Self {
        Self {
            installs,
            failure: Some(reason.into()),
        }
    }
}

impl FirmwareInstaller for RecordingInstaller {
    fn apply<'a>(
        &'a self,
        firmware_path: &'a Path,
        deployment: &'a Deployment,
    ) -> BoxFuture<'a, Result<(), DeploymentError>> {
        let installs = Arc::clone(&self.installs);
        let failure = self.failure.clone();
        let firmware_path = firmware_path.to_path_buf();
        let deployment_uuid = deployment.firmware_meta.uuid.clone();

        Box::pin(async move {
            let firmware_bytes = tokio::fs::read(&firmware_path)
                .await
                .map_err(DeploymentError::Io)?;

            installs.lock().unwrap().push(InstallRecord {
                firmware_path,
                firmware_bytes,
                deployment_uuid,
            });

            match failure {
                Some(reason) => Err(DeploymentError::Fwup(reason)),
                None => Ok(()),
            }
        })
    }
}

#[tokio::test]
async fn deployment_update_downloads_applies_and_reports_status() {
    let fixture = TestFixture::start().await;
    let installs = Arc::new(Mutex::new(Vec::new()));
    let installer = RecordingInstaller::successful(Arc::clone(&installs));
    let result = run_client_against_update_server(&fixture, installer).await;

    result.client_result.unwrap();
    assert_status_sequence(
        &result.server_messages,
        &[
            ("status_update", json!({"status": "received"})),
            ("status_update", json!({"status": "started"})),
            (
                "fwup_progress",
                json!({"stage": "downloading", "value": 100}),
            ),
            ("fwup_progress", json!({"stage": "updating", "value": 100})),
            ("status_update", json!({"status": "completed"})),
        ],
    );

    let installs = installs.lock().unwrap();
    assert_eq!(installs.len(), 1);
    assert_eq!(installs[0].firmware_bytes, FIRMWARE_BODY);
    assert_eq!(installs[0].deployment_uuid, TEST_UUID);
    assert!(installs[0]
        .firmware_path
        .ends_with(format!("firmware-{TEST_UUID}.fw")));

    assert_has_deployment_available(&result.client_events);
    assert!(result
        .client_events
        .iter()
        .any(|event| matches!(event, ClientEvent::FirmwareDownloaded(_))));
    assert!(result
        .client_events
        .iter()
        .any(|event| matches!(event, ClientEvent::FirmwareApplied)));
}

#[tokio::test]
async fn deployment_update_reports_failed_when_installer_fails() {
    let fixture = TestFixture::start().await;
    let installs = Arc::new(Mutex::new(Vec::new()));
    let installer = RecordingInstaller::failing(Arc::clone(&installs), "installer failed");
    let result = run_client_against_update_server(&fixture, installer).await;

    let error = result.client_result.unwrap_err();
    assert!(error.to_string().contains("fwup failed: installer failed"));
    assert_status_sequence(
        &result.server_messages,
        &[
            ("status_update", json!({"status": "received"})),
            ("status_update", json!({"status": "started"})),
            (
                "fwup_progress",
                json!({"stage": "downloading", "value": 100}),
            ),
            (
                "status_update",
                json!({"status": "failed", "reason": "FWUP error: installer failed"}),
            ),
        ],
    );

    assert_eq!(installs.lock().unwrap().len(), 1);
    assert_has_deployment_available(&result.client_events);
    assert!(result
        .client_events
        .iter()
        .any(|event| matches!(event, ClientEvent::FirmwareDownloaded(_))));
    assert!(!result
        .client_events
        .iter()
        .any(|event| matches!(event, ClientEvent::FirmwareApplied)));
}

struct TestFixture {
    _temp_dir: TempDir,
    config: Config,
    deployment_manager_options: DeploymentOptions,
    firmware_url: String,
}

impl TestFixture {
    async fn start() -> Self {
        let temp_dir = tempfile::tempdir().unwrap();
        let firmware_url = spawn_firmware_server().await;
        let config = Config {
            host: "ws://127.0.0.1:0".to_string(),
            auth: AuthConfig::SharedSecret {
                key: "test-key".to_string(),
                secret: "test-secret".to_string(),
            },
            serial_number: Some("integration-device-001".to_string()),
            fwup_devpath: None,
            fwup_task: None,
            firmware: Some(FirmwareMetadata {
                uuid: "current-fw".to_string(),
                version: "1.0.0".to_string(),
                platform: "x86_64".to_string(),
                architecture: "x86_64".to_string(),
                product: "integration-test".to_string(),
            }),
            heartbeat_interval_secs: Some(300),
            data_dir: Some(temp_dir.path().join("data")),
            device_api_version: None,
            console_version: None,
            fwup_version: None,
            currently_downloading_uuid: None,
            firmware_validated: None,
            firmware_auto_revert_detected: None,
            join_params: None,
            console: None,
        };
        let deployment_manager_options = DeploymentOptions {
            data_dir: temp_dir.path().join("data"),
        };

        Self {
            _temp_dir: temp_dir,
            config,
            deployment_manager_options,
            firmware_url,
        }
    }
}

struct RunResult {
    client_result: Result<(), link::client::ClientError>,
    client_events: Vec<ClientEvent>,
    server_messages: Vec<Message>,
}

async fn run_client_against_update_server(
    fixture: &TestFixture,
    installer: RecordingInstaller,
) -> RunResult {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let server_addr = listener.local_addr().unwrap();
    let firmware_url = fixture.firmware_url.clone();
    let server = tokio::spawn(async move { run_update_server(listener, firmware_url).await });

    let mut config = fixture.config.clone();
    config.host = format!("ws://{server_addr}");
    let deployment_manager =
        DeploymentManager::with_installer(fixture.deployment_manager_options.clone(), installer);
    let client = LinkClient::new(config)
        .unwrap()
        .with_deployment_manager(deployment_manager);
    let (event_tx, event_rx) = mpsc::channel(32);
    let client = tokio::spawn(async move { client.run(event_tx).await });

    let server_messages = timeout(Duration::from_secs(5), server)
        .await
        .unwrap()
        .unwrap();
    let client_result = timeout(Duration::from_secs(5), client)
        .await
        .unwrap()
        .unwrap();

    RunResult {
        client_result,
        client_events: collect_events(event_rx).await,
        server_messages,
    }
}

async fn run_update_server(listener: TcpListener, firmware_url: String) -> Vec<Message> {
    let (stream, _) = listener.accept().await.unwrap();
    let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();

    let join = read_text_message(&mut ws).await;
    assert_eq!(join.event, "phx_join");
    assert_eq!(join.topic, "device");
    send_join_reply(&mut ws, &join).await;

    let update = json!([
        null,
        null,
        "device",
        "update",
        {
            "firmware_url": firmware_url,
            "firmware_meta": {
                "uuid": TEST_UUID,
                "version": "2.0.0",
                "platform": "x86_64",
                "architecture": "x86_64",
                "product": "integration-test"
            }
        }
    ]);
    ws.send(tungstenite::Message::Text(update.to_string().into()))
        .await
        .unwrap();

    let mut messages = Vec::new();
    loop {
        let message = read_text_message(&mut ws).await;
        let should_close = is_terminal_deployment_message(&message);
        messages.push(message);
        if should_close {
            ws.close(None).await.unwrap();
            return messages;
        }
    }
}

async fn spawn_firmware_server() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut request = [0; 1024];
        let _ = stream.read(&mut request).await.unwrap();
        let headers = format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            FIRMWARE_BODY.len()
        );
        stream.write_all(headers.as_bytes()).await.unwrap();
        stream.write_all(FIRMWARE_BODY).await.unwrap();
    });

    format!("http://{addr}/firmware.fw")
}

async fn read_text_message(
    ws: &mut tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>,
) -> Message {
    match timeout(Duration::from_secs(5), ws.next()).await.unwrap() {
        Some(Ok(tungstenite::Message::Text(text))) => Message::from_json(&text).unwrap(),
        Some(Ok(message)) => panic!("expected websocket text message, got {message:?}"),
        Some(Err(error)) => panic!("websocket error: {error}"),
        None => panic!("websocket closed"),
    }
}

async fn send_join_reply(
    ws: &mut tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>,
    join: &Message,
) {
    let reply = json!([
        join.join_ref,
        join.msg_ref,
        join.topic,
        "phx_reply",
        {"status": "ok", "response": {}}
    ]);
    ws.send(tungstenite::Message::Text(reply.to_string().into()))
        .await
        .unwrap();
}

fn is_terminal_deployment_message(message: &Message) -> bool {
    message.event == "status_update"
        && matches!(
            message.payload.get("status").and_then(Value::as_str),
            Some("completed" | "failed")
        )
}

async fn collect_events(mut event_rx: mpsc::Receiver<ClientEvent>) -> Vec<ClientEvent> {
    let mut events = Vec::new();
    while let Ok(event) = event_rx.try_recv() {
        events.push(event);
    }
    events
}

fn assert_status_sequence(messages: &[Message], expected: &[(&str, Value)]) {
    let actual: Vec<(&str, Value)> = messages
        .iter()
        .map(|message| {
            (
                message.event.as_str(),
                normalized_payload(&message.event, &message.payload),
            )
        })
        .collect();

    assert_eq!(actual, expected);
}

fn normalized_payload(event: &str, payload: &Value) -> Value {
    match event {
        "status_update" => {
            let mut value = json!({"status": payload["status"]});
            if let Some(reason) = payload.get("reason") {
                value["reason"] = reason.clone();
            }
            value
        }
        "fwup_progress" => json!({
            "stage": payload["stage"],
            "value": payload["value"],
        }),
        _ => payload.clone(),
    }
}

fn assert_has_deployment_available(events: &[ClientEvent]) {
    assert!(events.iter().any(|event| match event {
        ClientEvent::DeploymentAvailable(deployment) => deployment.firmware_meta.uuid == TEST_UUID,
        _ => false,
    }));
}
