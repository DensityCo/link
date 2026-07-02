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
                    error!(error = %error, "connection error");
                    let _ = event_tx
                        .send(ClientEvent::Disconnected(error.to_string()))
                        .await;
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
}
