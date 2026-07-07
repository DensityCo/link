use crate::client::{ClientError, ClientEvent};
use crate::config::ScriptsConfig;
use crate::device_reporter::DeviceReporter;
use crate::protocol::Message;
use futures_util::future::BoxFuture;
use serde_json::Value;
use std::process::{ExitStatus, Stdio};
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::process::Command;
use tokio::sync::mpsc;
use tokio::task::{JoinHandle, JoinSet};
use tracing::{info, warn};

const DEFAULT_SCRIPT_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScriptRequest {
    pub script_ref: String,
    pub text: String,
    pub timeout: Duration,
}

impl ScriptRequest {
    fn from_payload(
        payload: &Value,
        default_timeout: Duration,
    ) -> Result<Self, ScriptRequestError> {
        let script_ref = payload
            .get("ref")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|script_ref| !script_ref.is_empty())
            .map(str::to_string);

        let text = payload
            .get("text")
            .and_then(Value::as_str)
            .map(str::to_string)
            .ok_or_else(|| ScriptRequestError {
                script_ref: script_ref.clone(),
                reason: "missing script text".to_string(),
            })?;

        let script_ref = script_ref.ok_or_else(|| ScriptRequestError {
            script_ref: None,
            reason: "missing script ref".to_string(),
        })?;

        let timeout = payload
            .get("timeout")
            .and_then(timeout_from_value)
            .unwrap_or(default_timeout);

        Ok(Self {
            script_ref,
            text,
            timeout,
        })
    }
}

#[derive(Debug, Clone)]
pub struct ScriptOutput {
    pub output: String,
    pub return_value: String,
}

#[derive(Debug, Error)]
pub enum ScriptError {
    #[error("script timed out")]
    Timeout,
    #[error("failed to start script command `{command}`: {source}")]
    Spawn {
        command: String,
        #[source]
        source: std::io::Error,
    },
    #[error("script command `{command}` failed with status {status}: {output}")]
    Command {
        command: String,
        status: ExitStatus,
        output: String,
    },
    #[error("{context}: {source}")]
    Io {
        context: &'static str,
        #[source]
        source: std::io::Error,
    },
    #[error("{context}: {source}")]
    Task {
        context: &'static str,
        #[source]
        source: tokio::task::JoinError,
    },
}

impl ScriptError {
    fn result_reason(&self) -> String {
        match self {
            ScriptError::Timeout => "timeout".to_string(),
            other => other.to_string(),
        }
    }

    fn result_output(&self) -> String {
        match self {
            ScriptError::Timeout => "Error running script: timeout exceeded".to_string(),
            other => format!("Error running script: {other}"),
        }
    }
}

pub trait ScriptRunner: Send + Sync {
    fn run<'a>(
        &'a self,
        request: ScriptRequest,
    ) -> BoxFuture<'a, Result<ScriptOutput, ScriptError>>;
}

#[derive(Debug, Clone)]
pub struct CommandScriptRunner {
    command: String,
    args: Vec<String>,
}

impl CommandScriptRunner {
    pub fn new(command: impl Into<String>, args: Vec<String>) -> Self {
        Self {
            command: command.into(),
            args,
        }
    }

    pub fn command(&self) -> &str {
        &self.command
    }

    pub fn args(&self) -> &[String] {
        &self.args
    }
}

impl ScriptRunner for CommandScriptRunner {
    fn run<'a>(
        &'a self,
        request: ScriptRequest,
    ) -> BoxFuture<'a, Result<ScriptOutput, ScriptError>> {
        Box::pin(async move {
            info!(
                script_ref = %request.script_ref,
                command = %self.command,
                args = ?self.args,
                "executing support script"
            );

            let mut child = Command::new(&self.command)
                .args(&self.args)
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
                .map_err(|source| ScriptError::Spawn {
                    command: self.command.clone(),
                    source,
                })?;

            let stdin = child.stdin.take();
            let stdout = child.stdout.take();
            let stderr = child.stderr.take();

            let stdin_handle = spawn_stdin_writer(stdin, request.text.clone());
            let stdout_handle = spawn_pipe_reader(stdout, "read script stdout")?;
            let stderr_handle = spawn_pipe_reader(stderr, "read script stderr")?;

            let status = tokio::select! {
                status = child.wait() => status.map_err(|source| ScriptError::Io {
                    context: "wait for script command",
                    source,
                })?,
                _ = tokio::time::sleep(request.timeout) => {
                    stdin_handle.abort();
                    stdout_handle.abort();
                    stderr_handle.abort();
                    let _ = child.kill().await;
                    let _ = child.wait().await;
                    return Err(ScriptError::Timeout);
                }
            };

            handle_stdin_result(stdin_handle).await?;
            let stdout = read_pipe_result(stdout_handle, "join script stdout reader").await?;
            let stderr = read_pipe_result(stderr_handle, "join script stderr reader").await?;
            let output = combined_output(&stdout, &stderr);

            if !status.success() {
                return Err(ScriptError::Command {
                    command: self.command.clone(),
                    status,
                    output,
                });
            }

            Ok(ScriptOutput {
                output,
                return_value: status.to_string(),
            })
        })
    }
}

#[derive(Clone)]
pub(crate) struct ScriptController {
    runner: Option<Arc<dyn ScriptRunner>>,
    default_timeout: Duration,
}

impl ScriptController {
    pub(crate) fn from_config(config: Option<&ScriptsConfig>) -> Self {
        let Some(config) = config else {
            return Self::disabled();
        };

        if !config.enabled() {
            return Self::disabled();
        }

        let Some(command) = config.command() else {
            return Self::disabled();
        };

        Self {
            runner: Some(Arc::new(CommandScriptRunner::new(
                command.to_string(),
                config.args().to_vec(),
            ))),
            default_timeout: config.timeout(),
        }
    }

    pub(crate) fn with_runner<R>(runner: R) -> Self
    where
        R: ScriptRunner + 'static,
    {
        Self {
            runner: Some(Arc::new(runner)),
            default_timeout: DEFAULT_SCRIPT_TIMEOUT,
        }
    }

    pub(crate) fn disabled() -> Self {
        Self {
            runner: None,
            default_timeout: DEFAULT_SCRIPT_TIMEOUT,
        }
    }

    pub(crate) fn disable(&mut self) {
        self.runner = None;
    }

    pub(crate) fn set_runner<R>(&mut self, runner: R)
    where
        R: ScriptRunner + 'static,
    {
        *self = Self::with_runner(runner);
    }

    fn runner(&self) -> Option<Arc<dyn ScriptRunner>> {
        self.runner.clone()
    }

    fn default_timeout(&self) -> Duration {
        self.default_timeout
    }
}

pub(crate) struct ScriptHandler {
    controller: ScriptController,
    reporter: DeviceReporter,
    event_tx: mpsc::Sender<ClientEvent>,
    error_tx: mpsc::Sender<ClientError>,
    active_scripts: JoinSet<()>,
}

impl ScriptHandler {
    pub(crate) fn new(
        controller: ScriptController,
        reporter: DeviceReporter,
        event_tx: mpsc::Sender<ClientEvent>,
        error_tx: mpsc::Sender<ClientError>,
    ) -> Self {
        Self {
            controller,
            reporter,
            event_tx,
            error_tx,
            active_scripts: JoinSet::new(),
        }
    }

    pub(crate) async fn handle_run(&mut self, msg: Message) {
        self.drain_finished_scripts();
        let request =
            match ScriptRequest::from_payload(&msg.payload, self.controller.default_timeout()) {
                Ok(request) => request,
                Err(error) => {
                    warn!(error = %error.reason, "failed to parse script request");
                    if let Some(script_ref) = error.script_ref {
                        self.report_parse_error(script_ref, error.reason).await;
                    }
                    return;
                }
            };

        let _ = self
            .event_tx
            .send(ClientEvent::ScriptRequested(request.script_ref.clone()))
            .await;

        let Some(runner) = self.controller.runner() else {
            let reason = "scripts are not enabled".to_string();
            let output = "Error running script: scripts are not enabled".to_string();
            self.report_script_error(&request.script_ref, &reason, &output)
                .await;
            return;
        };

        let reporter = self.reporter.clone();
        let event_tx = self.event_tx.clone();
        let error_tx = self.error_tx.clone();
        self.active_scripts.spawn(async move {
            let script_ref = request.script_ref.clone();
            match runner.run(request).await {
                Ok(output) => {
                    let result = reporter
                        .script_completed(&script_ref, &output.output, &output.return_value)
                        .await;
                    if let Err(error) = result {
                        let _ = error_tx.send(error).await;
                        return;
                    }
                    let _ = event_tx
                        .send(ClientEvent::ScriptCompleted(script_ref))
                        .await;
                }
                Err(error) => {
                    let reason = error.result_reason();
                    let output = error.result_output();
                    let result = reporter.script_failed(&script_ref, &reason, &output).await;
                    if let Err(error) = result {
                        let _ = error_tx.send(error).await;
                        return;
                    }
                    let _ = event_tx
                        .send(ClientEvent::ScriptFailed { script_ref, reason })
                        .await;
                }
            }
        });
    }

    fn drain_finished_scripts(&mut self) {
        while let Some(result) = self.active_scripts.try_join_next() {
            if let Err(error) = result {
                warn!(error = %error, "script task ended unexpectedly");
            }
        }
    }

    async fn report_parse_error(&self, script_ref: String, reason: String) {
        let output = format!("Error running script: {reason}");
        self.report_script_error(&script_ref, &reason, &output)
            .await;
    }

    async fn report_script_error(&self, script_ref: &str, reason: &str, output: &str) {
        if let Err(error) = self
            .reporter
            .script_failed(script_ref, reason, output)
            .await
        {
            let _ = self.error_tx.send(error).await;
            return;
        }

        let _ = self
            .event_tx
            .send(ClientEvent::ScriptFailed {
                script_ref: script_ref.to_string(),
                reason: reason.to_string(),
            })
            .await;
    }
}

#[derive(Debug)]
struct ScriptRequestError {
    script_ref: Option<String>,
    reason: String,
}

fn timeout_from_value(value: &Value) -> Option<Duration> {
    if let Some(milliseconds) = value.as_u64() {
        return Some(Duration::from_millis(milliseconds));
    }

    value
        .as_str()
        .and_then(|value| value.parse::<u64>().ok())
        .map(Duration::from_millis)
}

fn spawn_stdin_writer(
    stdin: Option<tokio::process::ChildStdin>,
    script: String,
) -> JoinHandle<Result<(), std::io::Error>> {
    tokio::spawn(async move {
        let Some(mut stdin) = stdin else {
            return Ok(());
        };

        stdin.write_all(script.as_bytes()).await
    })
}

fn spawn_pipe_reader(
    pipe: Option<impl tokio::io::AsyncRead + Unpin + Send + 'static>,
    context: &'static str,
) -> Result<JoinHandle<Result<Vec<u8>, std::io::Error>>, ScriptError> {
    let Some(mut pipe) = pipe else {
        return Err(ScriptError::Io {
            context,
            source: std::io::Error::other("missing pipe"),
        });
    };

    Ok(tokio::spawn(async move {
        let mut output = Vec::new();
        pipe.read_to_end(&mut output).await?;
        Ok(output)
    }))
}

async fn handle_stdin_result(
    handle: JoinHandle<Result<(), std::io::Error>>,
) -> Result<(), ScriptError> {
    match handle.await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(error)) if error.kind() == std::io::ErrorKind::BrokenPipe => Ok(()),
        Ok(Err(source)) => Err(ScriptError::Io {
            context: "write script stdin",
            source,
        }),
        Err(source) => Err(ScriptError::Task {
            context: "join script stdin writer",
            source,
        }),
    }
}

async fn read_pipe_result(
    handle: JoinHandle<Result<Vec<u8>, std::io::Error>>,
    context: &'static str,
) -> Result<Vec<u8>, ScriptError> {
    match handle.await {
        Ok(Ok(output)) => Ok(output),
        Ok(Err(source)) => Err(ScriptError::Io { context, source }),
        Err(source) => Err(ScriptError::Task { context, source }),
    }
}

fn combined_output(stdout: &[u8], stderr: &[u8]) -> String {
    let stdout = String::from_utf8_lossy(stdout).trim_end().to_string();
    let stderr = String::from_utf8_lossy(stderr).trim_end().to_string();

    match (stdout.is_empty(), stderr.is_empty()) {
        (true, true) => String::new(),
        (false, true) => stdout,
        (true, false) => stderr,
        (false, false) => format!("{stdout}\n{stderr}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parses_script_request() {
        let request = ScriptRequest::from_payload(
            &json!({
                "ref": "script-1",
                "text": "echo hi",
                "timeout": 2500
            }),
            DEFAULT_SCRIPT_TIMEOUT,
        )
        .unwrap();

        assert_eq!(request.script_ref, "script-1");
        assert_eq!(request.text, "echo hi");
        assert_eq!(request.timeout, Duration::from_millis(2500));
    }

    #[test]
    fn command_script_runner_stores_command_and_args() {
        let runner = CommandScriptRunner::new("/bin/sh", vec!["-s".to_string()]);

        assert_eq!(runner.command(), "/bin/sh");
        assert_eq!(runner.args(), &["-s".to_string()]);
    }
}
