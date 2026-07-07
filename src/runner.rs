use crate::client::{ClientError, ClientEvent, LinkClient};
use tokio::sync::mpsc;
use tracing::{error, info};

#[derive(Debug, Clone, Copy)]
pub struct RunnerOptions {
    pub max_backoff_secs: f64,
    pub jitter_factor: f64,
}

impl Default for RunnerOptions {
    fn default() -> Self {
        Self {
            max_backoff_secs: 60.0,
            jitter_factor: 0.5,
        }
    }
}

pub fn backoff_delay(attempt: u32, options: RunnerOptions) -> std::time::Duration {
    let base_secs = (2.0_f64).powi(attempt as i32).min(options.max_backoff_secs);
    let jitter = rand::random::<f64>() * base_secs * options.jitter_factor;
    std::time::Duration::from_secs_f64(base_secs + jitter)
}

pub struct LinkRunner {
    client: LinkClient,
    options: RunnerOptions,
}

impl LinkRunner {
    pub fn new(client: LinkClient) -> Self {
        Self {
            client,
            options: RunnerOptions::default(),
        }
    }

    pub fn with_options(client: LinkClient, options: RunnerOptions) -> Self {
        Self { client, options }
    }

    pub async fn run(&self, event_tx: mpsc::Sender<ClientEvent>) -> Result<(), ClientError> {
        let mut attempt: u32 = 0;

        loop {
            match self.client.run(event_tx.clone()).await {
                Ok(()) => {
                    info!("connection ended cleanly");
                    attempt = 0;
                }
                Err(error) => {
                    let reason = error.to_string();
                    if error.is_terminal() {
                        error!(error = %reason, "terminal client error; stopping runner");
                        let _ = event_tx.send(ClientEvent::Disconnected(reason)).await;
                        return Err(error);
                    }

                    error!(error = %reason, "connection error");
                    let _ = event_tx.send(ClientEvent::Disconnected(reason)).await;
                }
            }

            let delay = backoff_delay(attempt, self.options);
            info!(delay_secs = delay.as_secs_f64(), attempt, "reconnecting");
            tokio::time::sleep(delay).await;
            attempt = attempt.saturating_add(1).min(6);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{AuthConfig, Config};
    use crate::device::{DeviceInfo, DeviceInfoError, DeviceRuntimeState, FirmwareMetadata};
    use std::collections::BTreeMap;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use tokio::time::{timeout, Duration};

    fn test_config() -> Config {
        Config {
            host: "example.com".to_string(),
            auth: AuthConfig::SharedSecret {
                key: "test-key".to_string(),
                secret: "test-secret".to_string(),
            },
            device_info: None,
            serial_number: None,
            fwup_devpath: None,
            fwup_task: None,
            fwup_public_keys: None,
            firmware: None,
            heartbeat_interval_secs: None,
            data_dir: None,
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
        }
    }

    fn device_info(version: &str) -> DeviceInfo {
        DeviceInfo {
            serial_number: "device-001".to_string(),
            firmware: FirmwareMetadata {
                uuid: "fw-uuid".to_string(),
                version: version.to_string(),
                platform: "rpi4".to_string(),
                architecture: "arm".to_string(),
                product: "test-product".to_string(),
            },
            device_api_version: "2.3.0".to_string(),
            fwup_version: None,
            console_version: None,
            runtime_state: DeviceRuntimeState::default(),
            extra_join_params: BTreeMap::new(),
        }
    }

    #[test]
    fn backoff_delay_increases() {
        let options = RunnerOptions::default();
        let d0 = backoff_delay(0, options);
        let d1 = backoff_delay(1, options);
        let d3 = backoff_delay(3, options);

        assert!(d0.as_secs_f64() <= 1.5);
        assert!(d1.as_secs_f64() <= 3.0);
        assert!(d3.as_secs_f64() <= 12.0);
    }

    #[test]
    fn backoff_delay_caps() {
        let options = RunnerOptions::default();
        let d10 = backoff_delay(10, options);

        assert!(d10.as_secs_f64() <= 90.0);
    }

    #[tokio::test]
    async fn runner_stops_on_terminal_metadata_error() {
        let calls = Arc::new(AtomicUsize::new(0));
        let provider = {
            let calls = Arc::clone(&calls);
            move || {
                let call = calls.fetch_add(1, Ordering::SeqCst);
                if call == 0 {
                    Ok(device_info("1.0.0"))
                } else {
                    Err(DeviceInfoError::Missing("firmware.version"))
                }
            }
        };
        let client = LinkClient::from_provider(test_config(), provider).unwrap();
        let runner = LinkRunner::new(client);
        let (event_tx, mut event_rx) = mpsc::channel(1);

        let error = timeout(Duration::from_secs(1), runner.run(event_tx))
            .await
            .unwrap()
            .unwrap_err();

        assert!(matches!(error, ClientError::DeviceInfoProvider(_)));
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert!(matches!(
            event_rx.recv().await,
            Some(ClientEvent::Disconnected(reason)) if reason.contains("firmware.version")
        ));
    }
}
