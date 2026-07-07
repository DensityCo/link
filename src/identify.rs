use crate::config::IdentifyConfig;
use futures_util::future::BoxFuture;
use std::process::ExitStatus;
use std::sync::Arc;
use thiserror::Error;
use tokio::process::Command;
use tracing::info;

#[derive(Debug, Error)]
pub enum IdentifyError {
    #[error("failed to start identify command `{command}`: {source}")]
    Spawn {
        command: String,
        #[source]
        source: std::io::Error,
    },
    #[error("identify command `{command}` failed with status {status}: {output}")]
    Command {
        command: String,
        status: ExitStatus,
        output: String,
    },
}

pub trait IdentifyAction: Send + Sync {
    fn identify<'a>(&'a self) -> BoxFuture<'a, Result<(), IdentifyError>>;
}

#[derive(Debug, Clone)]
pub struct CommandIdentifyAction {
    command: String,
    args: Vec<String>,
}

impl CommandIdentifyAction {
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

impl IdentifyAction for CommandIdentifyAction {
    fn identify<'a>(&'a self) -> BoxFuture<'a, Result<(), IdentifyError>> {
        Box::pin(async move {
            info!(
                command = %self.command,
                args = ?self.args,
                "executing identify command"
            );

            let output = Command::new(&self.command)
                .args(&self.args)
                .output()
                .await
                .map_err(|source| IdentifyError::Spawn {
                    command: self.command.clone(),
                    source,
                })?;

            if output.status.success() {
                return Ok(());
            }

            let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
            let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
            let output_text = if stderr.is_empty() { stdout } else { stderr };

            Err(IdentifyError::Command {
                command: self.command.clone(),
                status: output.status,
                output: output_text,
            })
        })
    }
}

#[derive(Clone)]
pub(crate) struct IdentifyController {
    action: Option<Arc<dyn IdentifyAction>>,
}

impl IdentifyController {
    pub(crate) fn from_config(config: Option<&IdentifyConfig>) -> Self {
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
            action: Some(Arc::new(CommandIdentifyAction::new(
                command.to_string(),
                config.args().to_vec(),
            ))),
        }
    }

    pub(crate) fn with_action<A>(action: A) -> Self
    where
        A: IdentifyAction + 'static,
    {
        Self {
            action: Some(Arc::new(action)),
        }
    }

    pub(crate) fn disabled() -> Self {
        Self { action: None }
    }

    pub(crate) fn disable(&mut self) {
        *self = Self::disabled();
    }

    pub(crate) fn set_action<A>(&mut self, action: A)
    where
        A: IdentifyAction + 'static,
    {
        *self = Self::with_action(action);
    }

    pub(crate) fn is_enabled(&self) -> bool {
        self.action.is_some()
    }

    pub(crate) async fn execute(&self) -> Result<(), IdentifyError> {
        let Some(action) = &self.action else {
            return Ok(());
        };

        action.identify().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn command_identify_action_stores_command_and_args() {
        let action =
            CommandIdentifyAction::new("/usr/bin/identify-device", vec!["--blink".to_string()]);

        assert_eq!(action.command(), "/usr/bin/identify-device");
        assert_eq!(action.args(), &["--blink".to_string()]);
    }

    #[test]
    fn identify_controller_is_disabled_without_enabled_config() {
        let config = IdentifyConfig {
            enabled: Some(false),
            command: Some("/usr/bin/identify-device".to_string()),
            args: None,
        };

        assert!(!IdentifyController::from_config(Some(&config)).is_enabled());
    }
}
