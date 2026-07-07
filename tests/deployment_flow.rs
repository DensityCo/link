use futures_util::future::BoxFuture;
use futures_util::{SinkExt, StreamExt};
use link::{
    AuthConfig, ClientError, ClientEvent, Config, ConsoleBackend, ConsoleConfig, ConsoleError,
    ConsoleOutput, ConsoleSession, Deployment, DeploymentError, DeploymentManager,
    DeploymentOptions, FirmwareInstaller, FirmwareMetadata, HealthReport, HealthReporter,
    IdentifyAction, IdentifyError, LinkClient, RebootError, RebootReason, Rebooter, ScriptError,
    ScriptOutput, ScriptRequest, ScriptRunner, UPDATE_IN_PROGRESS_ALARM,
};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio::time::{sleep, timeout, Duration};

const FIRMWARE_BODY: &[u8] = b"test firmware bytes";
const TEST_UUID: &str = "integration-fw-uuid";

#[derive(Debug, Clone)]
struct WireMessage {
    join_ref: Option<String>,
    msg_ref: Option<String>,
    topic: String,
    event: String,
    payload: Value,
}

impl WireMessage {
    fn from_json(text: &str) -> Self {
        let arr: Vec<Value> = serde_json::from_str(text).unwrap();
        assert_eq!(arr.len(), 5, "invalid Phoenix channel message: {text}");
        Self {
            join_ref: arr[0].as_str().map(String::from),
            msg_ref: arr[1].as_str().map(String::from),
            topic: arr[2].as_str().unwrap().to_string(),
            event: arr[3].as_str().unwrap().to_string(),
            payload: arr[4].clone(),
        }
    }
}

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

#[derive(Clone)]
struct RecordingRebooter {
    reasons: Arc<Mutex<Vec<RebootReason>>>,
}

impl RecordingRebooter {
    fn new(reasons: Arc<Mutex<Vec<RebootReason>>>) -> Self {
        Self { reasons }
    }
}

impl Rebooter for RecordingRebooter {
    fn reboot<'a>(&'a self, reason: RebootReason) -> BoxFuture<'a, Result<(), RebootError>> {
        let reasons = Arc::clone(&self.reasons);

        Box::pin(async move {
            reasons.lock().unwrap().push(reason);
            Ok(())
        })
    }
}

#[derive(Clone)]
struct RecordingIdentifyAction {
    requests: Arc<Mutex<usize>>,
}

impl RecordingIdentifyAction {
    fn new(requests: Arc<Mutex<usize>>) -> Self {
        Self { requests }
    }
}

impl IdentifyAction for RecordingIdentifyAction {
    fn identify<'a>(&'a self) -> BoxFuture<'a, Result<(), IdentifyError>> {
        let requests = Arc::clone(&self.requests);

        Box::pin(async move {
            *requests.lock().unwrap() += 1;
            Ok(())
        })
    }
}

#[derive(Clone)]
struct RecordingScriptRunner {
    requests: Arc<Mutex<Vec<ScriptRequest>>>,
}

impl RecordingScriptRunner {
    fn new(requests: Arc<Mutex<Vec<ScriptRequest>>>) -> Self {
        Self { requests }
    }
}

impl ScriptRunner for RecordingScriptRunner {
    fn run<'a>(
        &'a self,
        request: ScriptRequest,
    ) -> BoxFuture<'a, Result<ScriptOutput, ScriptError>> {
        let requests = Arc::clone(&self.requests);

        Box::pin(async move {
            requests.lock().unwrap().push(request);
            Ok(ScriptOutput {
                output: "script output".to_string(),
                return_value: ":ok".to_string(),
            })
        })
    }
}

#[derive(Clone)]
struct TimeoutScriptRunner {
    requests: Arc<Mutex<Vec<ScriptRequest>>>,
}

impl TimeoutScriptRunner {
    fn new(requests: Arc<Mutex<Vec<ScriptRequest>>>) -> Self {
        Self { requests }
    }
}

impl ScriptRunner for TimeoutScriptRunner {
    fn run<'a>(
        &'a self,
        request: ScriptRequest,
    ) -> BoxFuture<'a, Result<ScriptOutput, ScriptError>> {
        let requests = Arc::clone(&self.requests);

        Box::pin(async move {
            requests.lock().unwrap().push(request);
            Err(ScriptError::Timeout)
        })
    }
}

#[derive(Clone)]
struct FixedHealthReporter;

impl HealthReporter for FixedHealthReporter {
    fn report(&self) -> HealthReport {
        let mut metrics = BTreeMap::new();
        metrics.insert("cpu_usage_percent".to_string(), 12.5);

        HealthReport {
            timestamp: "2026-07-06T00:00:00Z".to_string(),
            metrics,
            ..HealthReport::default()
        }
    }
}

#[derive(Clone)]
struct RecordingConsoleBackend {
    inputs: Arc<Mutex<Vec<String>>>,
}

impl RecordingConsoleBackend {
    fn new(inputs: Arc<Mutex<Vec<String>>>) -> Self {
        Self { inputs }
    }
}

impl ConsoleBackend for RecordingConsoleBackend {
    fn start(
        &self,
        output_tx: mpsc::Sender<ConsoleOutput>,
    ) -> Result<Box<dyn ConsoleSession>, ConsoleError> {
        Ok(Box::new(RecordingConsoleSession {
            inputs: Arc::clone(&self.inputs),
            output_tx,
            stopped: false,
        }))
    }
}

struct RecordingConsoleSession {
    inputs: Arc<Mutex<Vec<String>>>,
    output_tx: mpsc::Sender<ConsoleOutput>,
    stopped: bool,
}

impl ConsoleSession for RecordingConsoleSession {
    fn write_input(&mut self, data: &str) -> Result<(), ConsoleError> {
        self.inputs.lock().unwrap().push(data.to_string());
        self.output_tx
            .try_send(ConsoleOutput {
                data: format!("echo:{data}"),
            })
            .map_err(|error| ConsoleError::Write(error.to_string()))
    }

    fn resize(&mut self, _rows: u16, _cols: u16) -> Result<(), ConsoleError> {
        Ok(())
    }

    fn stop(&mut self) -> Result<(), ConsoleError> {
        self.stopped = true;
        Ok(())
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
            ("rebooting", json!({})),
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
    assert_eq!(result.reboot_reasons, vec![RebootReason::FirmwareApplied]);
    assert!(!result.alarms.contains_key(UPDATE_IN_PROGRESS_ALARM));
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
    assert!(result.reboot_reasons.is_empty());
    assert!(!result.alarms.contains_key(UPDATE_IN_PROGRESS_ALARM));
}

#[tokio::test]
async fn deployment_update_does_not_block_heartbeats_during_slow_download() {
    let mut fixture = TestFixture::start_with_firmware_delay(Duration::from_millis(2200)).await;
    fixture.config.heartbeat_interval_secs = Some(1);
    let installs = Arc::new(Mutex::new(Vec::new()));
    let installer = RecordingInstaller::successful(Arc::clone(&installs));
    let result = run_client_against_update_server(&fixture, installer).await;

    result.client_result.unwrap();
    assert!(result
        .server_messages
        .iter()
        .any(|message| message.topic == "phoenix" && message.event == "heartbeat"));
    assert!(result.server_messages.iter().any(|message| {
        message.event == "status_update"
            && message.payload.get("status").and_then(Value::as_str) == Some("completed")
    }));
    assert_eq!(result.reboot_reasons, vec![RebootReason::FirmwareApplied]);
}

#[tokio::test]
async fn server_reboot_command_reports_and_runs_rebooter() {
    let fixture = TestFixture::start().await;
    let result = run_client_against_reboot_server(&fixture).await;

    result.client_result.unwrap();
    assert_eq!(result.reboot_reasons, vec![RebootReason::ServerRequested]);
    assert!(result
        .client_events
        .iter()
        .any(|event| matches!(event, ClientEvent::RebootRequested)));
    assert_status_sequence(&result.server_messages, &[("rebooting", json!({}))]);
}

#[tokio::test]
async fn server_reboot_command_without_rebooter_does_not_report_rebooting() {
    let fixture = TestFixture::start().await;
    let result = run_client_against_reboot_server_without_rebooter(&fixture).await;

    result.client_result.unwrap();
    assert!(result.reboot_reasons.is_empty());
    assert!(result
        .client_events
        .iter()
        .any(|event| matches!(event, ClientEvent::RebootRequested)));
    assert!(result.server_messages.is_empty());
}

#[tokio::test]
async fn server_identify_command_runs_identify_action() {
    let fixture = TestFixture::start().await;
    let result = run_client_against_identify_server(&fixture).await;

    result.client_result.unwrap();
    assert_eq!(result.identify_requests, 1);
    assert!(result
        .client_events
        .iter()
        .any(|event| matches!(event, ClientEvent::IdentifyRequested)));
}

#[tokio::test]
async fn server_identify_command_without_action_only_emits_event() {
    let fixture = TestFixture::start().await;
    let result = run_client_against_identify_server_without_action(&fixture).await;

    result.client_result.unwrap();
    assert_eq!(result.identify_requests, 0);
    assert!(result
        .client_events
        .iter()
        .any(|event| matches!(event, ClientEvent::IdentifyRequested)));
}

#[tokio::test]
async fn server_script_run_executes_runner_and_reports_result() {
    let fixture = TestFixture::start().await;
    let script_requests = Arc::new(Mutex::new(Vec::new()));
    let runner = RecordingScriptRunner::new(Arc::clone(&script_requests));
    let result = run_client_against_script_server(&fixture, runner).await;

    result.client_result.unwrap();
    let script_requests = script_requests.lock().unwrap();
    assert_eq!(script_requests.len(), 1);
    assert_eq!(script_requests[0].script_ref, "script-1");
    assert_eq!(script_requests[0].text, "echo hi");
    assert_eq!(script_requests[0].timeout, Duration::from_secs(1));

    assert!(result
        .client_events
        .iter()
        .any(|event| matches!(event, ClientEvent::ScriptRequested(script_ref) if script_ref == "script-1")));
    assert!(result
        .client_events
        .iter()
        .any(|event| matches!(event, ClientEvent::ScriptCompleted(script_ref) if script_ref == "script-1")));
    assert_status_sequence(
        &result.server_messages,
        &[(
            "scripts/run",
            json!({
                "ref": "script-1",
                "result": "completed",
                "output": "script output",
                "return": ":ok",
            }),
        )],
    );
}

#[tokio::test]
async fn server_script_run_without_runner_reports_disabled_error() {
    let fixture = TestFixture::start().await;
    let result = run_client_against_script_server_without_runner(&fixture).await;

    result.client_result.unwrap();
    assert!(result
        .client_events
        .iter()
        .any(|event| matches!(event, ClientEvent::ScriptRequested(script_ref) if script_ref == "script-1")));
    assert!(result.client_events.iter().any(|event| matches!(
        event,
        ClientEvent::ScriptFailed { script_ref, reason }
            if script_ref == "script-1" && reason == "scripts are not enabled"
    )));
    assert_status_sequence(
        &result.server_messages,
        &[(
            "scripts/run",
            json!({
                "ref": "script-1",
                "result": "error",
                "reason": "scripts are not enabled",
                "output": "Error running script: scripts are not enabled",
                "return": "",
            }),
        )],
    );
}

#[tokio::test]
async fn server_script_runner_error_reports_error_without_failing_connection() {
    let fixture = TestFixture::start().await;
    let script_requests = Arc::new(Mutex::new(Vec::new()));
    let runner = TimeoutScriptRunner::new(Arc::clone(&script_requests));
    let result = run_client_against_script_server(&fixture, runner).await;

    result.client_result.unwrap();
    assert_eq!(script_requests.lock().unwrap().len(), 1);
    assert!(result.client_events.iter().any(|event| matches!(
        event,
        ClientEvent::ScriptFailed { script_ref, reason }
            if script_ref == "script-1" && reason == "timeout"
    )));
    assert_status_sequence(
        &result.server_messages,
        &[(
            "scripts/run",
            json!({
                "ref": "script-1",
                "result": "error",
                "reason": "timeout",
                "output": "Error running script: timeout exceeded",
                "return": "",
            }),
        )],
    );
}

#[tokio::test]
async fn health_extension_attaches_and_reports() {
    let fixture = TestFixture::start().await;
    let result = run_client_against_health_server(&fixture).await;

    result.client_result.unwrap();
    assert!(result
        .client_events
        .iter()
        .any(|event| matches!(event, ClientEvent::ExtensionsJoined)));
    assert!(result
        .client_events
        .iter()
        .any(|event| matches!(event, ClientEvent::HealthReported)));

    assert_eq!(result.server_messages[0].topic, "extensions");
    assert_eq!(result.server_messages[0].event, "phx_join");
    assert_eq!(result.server_messages[1].event, "health:attached");
    assert_eq!(result.server_messages[2].event, "health:report");
    assert_eq!(
        result.server_messages[2].payload["value"]["metrics"]["cpu_usage_percent"],
        json!(12.5)
    );
    assert_eq!(
        result.server_messages[2].payload["value"]["alarms"]["link.test_alarm"],
        json!("test alarm")
    );
    assert_eq!(
        result.alarms.get("link.test_alarm").map(String::as_str),
        Some("test alarm")
    );
}

#[tokio::test]
async fn console_starts_session_and_forwards_output() {
    let mut fixture = TestFixture::start().await;
    fixture.config.console = Some(ConsoleConfig {
        enabled: Some(true),
        version: Some("2.0.0".to_string()),
        command: None,
        args: None,
        timeout_secs: Some(300),
        rows: None,
        cols: None,
    });

    let inputs = Arc::new(Mutex::new(Vec::new()));
    let backend = RecordingConsoleBackend::new(Arc::clone(&inputs));
    let result = run_client_against_console_server(&fixture, backend).await;

    result.client_result.unwrap();
    assert_eq!(*inputs.lock().unwrap(), vec!["ping\n".to_string()]);
    assert!(result
        .client_events
        .iter()
        .any(|event| matches!(event, ClientEvent::ConsoleJoined)));
    assert!(result
        .client_events
        .iter()
        .any(|event| matches!(event, ClientEvent::ConsoleStarted)));

    assert_eq!(result.server_messages[0].topic, "console");
    assert_eq!(result.server_messages[0].event, "phx_join");
    assert_eq!(result.server_messages[1].topic, "console");
    assert_eq!(result.server_messages[1].event, "up");
    assert_eq!(
        result.server_messages[1].payload["data"],
        json!("echo:ping\n")
    );
}

struct TestFixture {
    _temp_dir: TempDir,
    config: Config,
    deployment_manager_options: DeploymentOptions,
    firmware_url: String,
}

impl TestFixture {
    async fn start() -> Self {
        Self::start_with_firmware_delay(Duration::ZERO).await
    }

    async fn start_with_firmware_delay(response_delay: Duration) -> Self {
        let temp_dir = tempfile::tempdir().unwrap();
        let firmware_url = spawn_firmware_server(response_delay).await;
        let config = Config {
            host: "ws://127.0.0.1:0".to_string(),
            auth: AuthConfig::SharedSecret {
                key: "test-key".to_string(),
                secret: "test-secret".to_string(),
            },
            serial_number: Some("integration-device-001".to_string()),
            fwup_devpath: None,
            fwup_task: None,
            fwup_public_keys: None,
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
            reboot: None,
            identify: None,
            scripts: None,
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
    client_result: Result<(), ClientError>,
    client_events: Vec<ClientEvent>,
    server_messages: Vec<WireMessage>,
    reboot_reasons: Vec<RebootReason>,
    identify_requests: usize,
    alarms: BTreeMap<String, String>,
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
    let reboot_reasons = Arc::new(Mutex::new(Vec::new()));
    let rebooter = RecordingRebooter::new(Arc::clone(&reboot_reasons));
    let client = LinkClient::new(config)
        .unwrap()
        .with_deployment_manager(deployment_manager)
        .with_rebooter(rebooter);
    let alarm_store = client.alarm_store();
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
    let reboot_reasons = reboot_reasons.lock().unwrap().clone();
    let alarms = alarm_store.snapshot();

    RunResult {
        client_result,
        client_events: collect_events(event_rx).await,
        server_messages,
        reboot_reasons,
        identify_requests: 0,
        alarms,
    }
}

async fn run_client_against_health_server(fixture: &TestFixture) -> RunResult {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let server_addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move { run_health_server(listener).await });

    let mut config = fixture.config.clone();
    config.host = format!("ws://{server_addr}");
    let mut client = LinkClient::new(config).unwrap();
    client.set_health_reporter(FixedHealthReporter);
    let alarm_store = client.alarm_store();
    alarm_store.set("link.test_alarm", "test alarm");
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
    let alarms = alarm_store.snapshot();

    RunResult {
        client_result,
        client_events: collect_events(event_rx).await,
        server_messages,
        reboot_reasons: Vec::new(),
        identify_requests: 0,
        alarms,
    }
}

async fn run_client_against_console_server(
    fixture: &TestFixture,
    backend: RecordingConsoleBackend,
) -> RunResult {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let server_addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move { run_console_server(listener).await });

    let mut config = fixture.config.clone();
    config.host = format!("ws://{server_addr}");
    let mut client = LinkClient::new(config).unwrap();
    client.set_console_backend(backend);
    let alarm_store = client.alarm_store();
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
    let alarms = alarm_store.snapshot();

    RunResult {
        client_result,
        client_events: collect_events(event_rx).await,
        server_messages,
        reboot_reasons: Vec::new(),
        identify_requests: 0,
        alarms,
    }
}

async fn run_client_against_reboot_server(fixture: &TestFixture) -> RunResult {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let server_addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move { run_reboot_server(listener).await });

    let mut config = fixture.config.clone();
    config.host = format!("ws://{server_addr}");
    let reboot_reasons = Arc::new(Mutex::new(Vec::new()));
    let rebooter = RecordingRebooter::new(Arc::clone(&reboot_reasons));
    let client = LinkClient::new(config).unwrap().with_rebooter(rebooter);
    let alarm_store = client.alarm_store();
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
    let reboot_reasons = reboot_reasons.lock().unwrap().clone();
    let alarms = alarm_store.snapshot();

    RunResult {
        client_result,
        client_events: collect_events(event_rx).await,
        server_messages,
        reboot_reasons,
        identify_requests: 0,
        alarms,
    }
}

async fn run_client_against_reboot_server_without_rebooter(fixture: &TestFixture) -> RunResult {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let server_addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move { run_reboot_server_without_reply(listener).await });

    let mut config = fixture.config.clone();
    config.host = format!("ws://{server_addr}");
    let client = LinkClient::new(config).unwrap();
    let alarm_store = client.alarm_store();
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
    let alarms = alarm_store.snapshot();

    RunResult {
        client_result,
        client_events: collect_events(event_rx).await,
        server_messages,
        reboot_reasons: Vec::new(),
        identify_requests: 0,
        alarms,
    }
}

async fn run_client_against_identify_server(fixture: &TestFixture) -> RunResult {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let server_addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move { run_identify_server(listener).await });

    let mut config = fixture.config.clone();
    config.host = format!("ws://{server_addr}");
    let identify_requests = Arc::new(Mutex::new(0usize));
    let identify_action = RecordingIdentifyAction::new(Arc::clone(&identify_requests));
    let client = LinkClient::new(config)
        .unwrap()
        .with_identify_action(identify_action);
    let alarm_store = client.alarm_store();
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
    let identify_requests = *identify_requests.lock().unwrap();
    let alarms = alarm_store.snapshot();

    RunResult {
        client_result,
        client_events: collect_events(event_rx).await,
        server_messages,
        reboot_reasons: Vec::new(),
        identify_requests,
        alarms,
    }
}

async fn run_client_against_identify_server_without_action(fixture: &TestFixture) -> RunResult {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let server_addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move { run_identify_server(listener).await });

    let mut config = fixture.config.clone();
    config.host = format!("ws://{server_addr}");
    let client = LinkClient::new(config).unwrap();
    let alarm_store = client.alarm_store();
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
    let alarms = alarm_store.snapshot();

    RunResult {
        client_result,
        client_events: collect_events(event_rx).await,
        server_messages,
        reboot_reasons: Vec::new(),
        identify_requests: 0,
        alarms,
    }
}

async fn run_client_against_script_server(
    fixture: &TestFixture,
    runner: impl ScriptRunner + 'static,
) -> RunResult {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let server_addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move { run_script_server(listener).await });

    let mut config = fixture.config.clone();
    config.host = format!("ws://{server_addr}");
    let client = LinkClient::new(config).unwrap().with_script_runner(runner);
    let alarm_store = client.alarm_store();
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
    let alarms = alarm_store.snapshot();

    RunResult {
        client_result,
        client_events: collect_events(event_rx).await,
        server_messages,
        reboot_reasons: Vec::new(),
        identify_requests: 0,
        alarms,
    }
}

async fn run_client_against_script_server_without_runner(fixture: &TestFixture) -> RunResult {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let server_addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move { run_script_server(listener).await });

    let mut config = fixture.config.clone();
    config.host = format!("ws://{server_addr}");
    let client = LinkClient::new(config).unwrap();
    let alarm_store = client.alarm_store();
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
    let alarms = alarm_store.snapshot();

    RunResult {
        client_result,
        client_events: collect_events(event_rx).await,
        server_messages,
        reboot_reasons: Vec::new(),
        identify_requests: 0,
        alarms,
    }
}

async fn run_update_server(listener: TcpListener, firmware_url: String) -> Vec<WireMessage> {
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
    ws.send(tungstenite::Message::Text(update.to_string()))
        .await
        .unwrap();

    let mut messages = Vec::new();
    loop {
        let message = read_text_message(&mut ws).await;
        let should_close = should_close_update_server(&message);
        messages.push(message);
        if should_close {
            ws.close(None).await.unwrap();
            return messages;
        }
    }
}

async fn run_reboot_server(listener: TcpListener) -> Vec<WireMessage> {
    let (stream, _) = listener.accept().await.unwrap();
    let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();

    let join = read_text_message(&mut ws).await;
    assert_eq!(join.event, "phx_join");
    assert_eq!(join.topic, "device");
    send_join_reply(&mut ws, &join).await;

    ws.send(tungstenite::Message::Text(
        json!([null, null, "device", "reboot", {}]).to_string(),
    ))
    .await
    .unwrap();

    let rebooting = read_text_message(&mut ws).await;
    ws.close(None).await.unwrap();

    vec![rebooting]
}

async fn run_reboot_server_without_reply(listener: TcpListener) -> Vec<WireMessage> {
    let (stream, _) = listener.accept().await.unwrap();
    let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();

    let join = read_text_message(&mut ws).await;
    assert_eq!(join.event, "phx_join");
    assert_eq!(join.topic, "device");
    send_join_reply(&mut ws, &join).await;

    ws.send(tungstenite::Message::Text(
        json!([null, null, "device", "reboot", {}]).to_string(),
    ))
    .await
    .unwrap();

    sleep(Duration::from_millis(50)).await;
    ws.close(None).await.unwrap();

    Vec::new()
}

async fn run_identify_server(listener: TcpListener) -> Vec<WireMessage> {
    let (stream, _) = listener.accept().await.unwrap();
    let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();

    let join = read_text_message(&mut ws).await;
    assert_eq!(join.event, "phx_join");
    assert_eq!(join.topic, "device");
    send_join_reply(&mut ws, &join).await;

    ws.send(tungstenite::Message::Text(
        json!([null, null, "device", "identify", {}]).to_string(),
    ))
    .await
    .unwrap();

    sleep(Duration::from_millis(50)).await;
    ws.close(None).await.unwrap();

    Vec::new()
}

async fn run_script_server(listener: TcpListener) -> Vec<WireMessage> {
    let (stream, _) = listener.accept().await.unwrap();
    let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();

    let join = read_text_message(&mut ws).await;
    assert_eq!(join.event, "phx_join");
    assert_eq!(join.topic, "device");
    send_join_reply(&mut ws, &join).await;

    ws.send(tungstenite::Message::Text(
        json!([
            null,
            null,
            "device",
            "scripts/run",
            {"ref": "script-1", "text": "echo hi", "timeout": 1000}
        ])
        .to_string(),
    ))
    .await
    .unwrap();

    let result = read_text_message(&mut ws).await;
    ws.close(None).await.unwrap();

    vec![result]
}

async fn run_health_server(listener: TcpListener) -> Vec<WireMessage> {
    let (stream, _) = listener.accept().await.unwrap();
    let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();

    let join = read_text_message(&mut ws).await;
    assert_eq!(join.event, "phx_join");
    assert_eq!(join.topic, "device");
    send_join_reply(&mut ws, &join).await;

    ws.send(tungstenite::Message::Text(
        json!([null, null, "device", "extensions:get", {}]).to_string(),
    ))
    .await
    .unwrap();

    let extensions_join = read_text_message(&mut ws).await;
    assert_eq!(extensions_join.topic, "extensions");
    assert_eq!(extensions_join.event, "phx_join");
    send_extensions_join_reply(&mut ws, &extensions_join).await;

    let attached = read_text_message(&mut ws).await;
    let report = read_text_message(&mut ws).await;
    ws.close(None).await.unwrap();

    vec![extensions_join, attached, report]
}

async fn run_console_server(listener: TcpListener) -> Vec<WireMessage> {
    let (stream, _) = listener.accept().await.unwrap();
    let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();

    let join = read_text_message(&mut ws).await;
    assert_eq!(join.event, "phx_join");
    assert_eq!(join.topic, "device");
    send_join_reply(&mut ws, &join).await;

    let console_join = read_text_message(&mut ws).await;
    assert_eq!(console_join.topic, "console");
    assert_eq!(console_join.event, "phx_join");
    send_join_reply(&mut ws, &console_join).await;

    ws.send(tungstenite::Message::Text(
        json!([null, null, "console", "dn", {"data": "ping\n"}]).to_string(),
    ))
    .await
    .unwrap();

    let output = read_text_message(&mut ws).await;
    ws.close(None).await.unwrap();

    vec![console_join, output]
}

async fn spawn_firmware_server(response_delay: Duration) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut request = [0; 1024];
        let _ = stream.read(&mut request).await.unwrap();
        sleep(response_delay).await;
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
) -> WireMessage {
    match timeout(Duration::from_secs(5), ws.next()).await.unwrap() {
        Some(Ok(tungstenite::Message::Text(text))) => WireMessage::from_json(&text),
        Some(Ok(message)) => panic!("expected websocket text message, got {message:?}"),
        Some(Err(error)) => panic!("websocket error: {error}"),
        None => panic!("websocket closed"),
    }
}

async fn send_join_reply(
    ws: &mut tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>,
    join: &WireMessage,
) {
    let reply = json!([
        join.join_ref,
        join.msg_ref,
        join.topic,
        "phx_reply",
        {"status": "ok", "response": {}}
    ]);
    ws.send(tungstenite::Message::Text(reply.to_string()))
        .await
        .unwrap();
}

async fn send_extensions_join_reply(
    ws: &mut tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>,
    join: &WireMessage,
) {
    let reply = json!([
        join.join_ref,
        join.msg_ref,
        join.topic,
        "phx_reply",
        {"status": "ok", "response": {"health": "0.0.1"}}
    ]);
    ws.send(tungstenite::Message::Text(reply.to_string()))
        .await
        .unwrap();
}

fn should_close_update_server(message: &WireMessage) -> bool {
    if message.event == "rebooting" {
        return true;
    }

    message.event == "status_update"
        && message.payload.get("status").and_then(Value::as_str) == Some("failed")
}

async fn collect_events(mut event_rx: mpsc::Receiver<ClientEvent>) -> Vec<ClientEvent> {
    let mut events = Vec::new();
    while let Ok(event) = event_rx.try_recv() {
        events.push(event);
    }
    events
}

fn assert_status_sequence(messages: &[WireMessage], expected: &[(&str, Value)]) {
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
