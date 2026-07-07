use crate::config::RebootConfig;
use futures_util::future::BoxFuture;
use std::process::ExitStatus;
use std::sync::Arc;
use thiserror::Error;
use tokio::process::Command;
use tracing::info;

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum RebootReason {
    ServerRequested,
    FirmwareApplied,
}

#[derive(Debug, Error)]
pub enum RebootError {
    #[error("failed to start reboot command `{command}`: {source}")]
    Spawn {
        command: String,
        #[source]
        source: std::io::Error,
    },
    #[error("reboot command `{command}` failed with status {status}: {output}")]
    Command {
        command: String,
        status: ExitStatus,
        output: String,
    },
}

pub trait Rebooter: Send + Sync {
    fn reboot<'a>(&'a self, reason: RebootReason) -> BoxFuture<'a, Result<(), RebootError>>;
}

#[derive(Debug, Clone)]
pub struct CommandRebooter {
    command: String,
    args: Vec<String>,
}

impl CommandRebooter {
    pub fn new(command: impl Into<String>, args: Vec<String>) -> Self {
        Self {
            command: command.into(),
            args,
        }
    }

    pub fn system() -> Self {
        Self::new("reboot", Vec::new())
    }

    pub fn command(&self) -> &str {
        &self.command
    }

    pub fn args(&self) -> &[String] {
        &self.args
    }
}

impl Rebooter for CommandRebooter {
    fn reboot<'a>(&'a self, reason: RebootReason) -> BoxFuture<'a, Result<(), RebootError>> {
        Box::pin(async move {
            info!(
                reason = ?reason,
                command = %self.command,
                args = ?self.args,
                "executing reboot command"
            );

            let output = Command::new(&self.command)
                .args(&self.args)
                .output()
                .await
                .map_err(|source| RebootError::Spawn {
                    command: self.command.clone(),
                    source,
                })?;

            if output.status.success() {
                return Ok(());
            }

            let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
            let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
            let output_text = if stderr.is_empty() { stdout } else { stderr };

            Err(RebootError::Command {
                command: self.command.clone(),
                status: output.status,
                output: output_text,
            })
        })
    }
}

#[derive(Clone)]
pub(crate) struct RebootController {
    rebooter: Option<Arc<dyn Rebooter>>,
    after_firmware_apply: bool,
    on_server_request: bool,
}

impl RebootController {
    pub(crate) fn from_config(config: Option<&RebootConfig>) -> Self {
        let Some(config) = config else {
            return Self::disabled();
        };

        if !config.enabled() {
            return Self::disabled();
        }

        Self {
            rebooter: Some(Arc::new(CommandRebooter::new(
                config.command().to_string(),
                config.args().to_vec(),
            ))),
            after_firmware_apply: config.after_firmware_apply(),
            on_server_request: config.on_server_request(),
        }
    }

    pub(crate) fn with_rebooter<R>(rebooter: R) -> Self
    where
        R: Rebooter + 'static,
    {
        Self {
            rebooter: Some(Arc::new(rebooter)),
            after_firmware_apply: true,
            on_server_request: true,
        }
    }

    pub(crate) fn disabled() -> Self {
        Self {
            rebooter: None,
            after_firmware_apply: false,
            on_server_request: false,
        }
    }

    pub(crate) fn disable(&mut self) {
        *self = Self::disabled();
    }

    pub(crate) fn set_rebooter<R>(&mut self, rebooter: R)
    where
        R: Rebooter + 'static,
    {
        *self = Self::with_rebooter(rebooter);
    }

    pub(crate) fn is_enabled_for(&self, reason: RebootReason) -> bool {
        self.rebooter.is_some() && self.allows(reason)
    }

    pub(crate) async fn execute(&self, reason: RebootReason) -> Result<(), RebootError> {
        let Some(rebooter) = &self.rebooter else {
            return Ok(());
        };

        if !self.allows(reason) {
            return Ok(());
        }

        rebooter.reboot(reason).await
    }

    fn allows(&self, reason: RebootReason) -> bool {
        match reason {
            RebootReason::ServerRequested => self.on_server_request,
            RebootReason::FirmwareApplied => self.after_firmware_apply,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn command_rebooter_defaults_to_reboot_command() {
        let rebooter = CommandRebooter::system();

        assert_eq!(rebooter.command(), "reboot");
        assert!(rebooter.args().is_empty());
    }

    #[test]
    fn reboot_policy_follows_reboot_config() {
        let config = RebootConfig {
            enabled: Some(true),
            command: None,
            args: None,
            after_firmware_apply: Some(false),
            on_server_request: Some(true),
        };
        let controller = RebootController::from_config(Some(&config));

        assert!(!controller.is_enabled_for(RebootReason::FirmwareApplied));
        assert!(controller.is_enabled_for(RebootReason::ServerRequested));
    }
}
