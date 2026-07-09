use crate::alarms::{AlarmStore, UPDATE_IN_PROGRESS_ALARM};
use crate::client::{ClientError, ClientEvent};
use crate::deployment::{Deployment, DeploymentEvent, DeploymentManager};
use crate::device_reporter::DeviceReporter;
use crate::protocol::Message;
use crate::reboot::{RebootController, RebootReason};
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tracing::{info, warn};

pub(crate) struct DeploymentHandler {
    manager: DeploymentManager,
    alarm_store: AlarmStore,
    device_reporter: DeviceReporter,
    reboot_controller: RebootController,
    event_tx: mpsc::Sender<ClientEvent>,
    error_tx: mpsc::Sender<ClientError>,
    active_deployments: JoinSet<()>,
}

impl DeploymentHandler {
    pub(crate) fn new(
        manager: DeploymentManager,
        alarm_store: AlarmStore,
        device_reporter: DeviceReporter,
        reboot_controller: RebootController,
        event_tx: mpsc::Sender<ClientEvent>,
        error_tx: mpsc::Sender<ClientError>,
    ) -> Self {
        Self {
            manager,
            alarm_store,
            device_reporter,
            reboot_controller,
            event_tx,
            error_tx,
            active_deployments: JoinSet::new(),
        }
    }

    pub(crate) async fn handle_update(&mut self, msg: Message) {
        self.drain_finished_deployments();
        info!("received deployment request");
        let deployment = match Deployment::from_payload(&msg.payload) {
            Ok(deployment) => deployment,
            Err(error) => {
                warn!(error = %error, "failed to parse deployment message");
                return;
            }
        };

        let _ = self
            .event_tx
            .send(ClientEvent::DeploymentAvailable(Box::new(
                deployment.clone(),
            )))
            .await;
        let deployment_manager = self.manager.clone();
        let deployment_alarm_store = self.alarm_store.clone();
        let deployment_reporter = self.device_reporter.clone();
        let deployment_reboot_controller = self.reboot_controller.clone();
        let deployment_event_tx = self.event_tx.clone();
        let deployment_error_tx = self.error_tx.clone();
        self.active_deployments.spawn(async move {
            if let Err(error) = run_deployment(
                deployment_manager,
                deployment_alarm_store,
                deployment,
                deployment_reporter,
                deployment_reboot_controller,
                deployment_event_tx,
            )
            .await
            {
                let _ = deployment_error_tx.send(error).await;
            }
        });
    }

    fn drain_finished_deployments(&mut self) {
        while let Some(result) = self.active_deployments.try_join_next() {
            if let Err(error) = result {
                warn!(error = %error, "deployment task ended unexpectedly");
            }
        }
    }
}

async fn run_deployment(
    deployment_manager: DeploymentManager,
    alarm_store: AlarmStore,
    deployment: Deployment,
    reporter: DeviceReporter,
    reboot_controller: RebootController,
    event_tx: mpsc::Sender<ClientEvent>,
) -> Result<(), ClientError> {
    info!(
        uuid = %deployment.firmware_meta.uuid,
        version = %deployment.firmware_meta.version,
        "downloading firmware"
    );

    reporter.deployment_received().await?;
    alarm_store.set(UPDATE_IN_PROGRESS_ALARM, "firmware update is in progress");

    let (progress_tx, mut progress_rx) = mpsc::unbounded_channel::<DeploymentEvent>();
    let deployment_handle = tokio::spawn(async move {
        deployment_manager
            .run(deployment, move |event| {
                let _ = progress_tx.send(event);
            })
            .await
    });

    if let Err(error) = reporter.deployment_started(None).await {
        alarm_store.clear(UPDATE_IN_PROGRESS_ALARM);
        return Err(error);
    }

    let mut last_reported_percent: u8 = 0;
    while let Some(event) = progress_rx.recv().await {
        match event {
            DeploymentEvent::DownloadProgress(pct) => {
                let should_report = pct > last_reported_percent + 4 || pct == 100;

                if should_report {
                    last_reported_percent = pct;
                    if let Err(error) = reporter.download_progress(pct).await {
                        deployment_handle.abort();
                        alarm_store.clear(UPDATE_IN_PROGRESS_ALARM);
                        return Err(error);
                    }
                }
            }
            DeploymentEvent::FirmwareDownloaded(path) => {
                info!(path = %path.display(), "firmware downloaded");
                let _ = event_tx.send(ClientEvent::FirmwareDownloaded(path)).await;
            }
        }
    }

    let deployment_result = match deployment_handle.await {
        Ok(result) => result,
        Err(error) => {
            alarm_store.clear(UPDATE_IN_PROGRESS_ALARM);
            return Err(ClientError::Connection(format!(
                "deployment task failed: {}",
                error
            )));
        }
    };

    if let Err(error) = deployment_result {
        let reason = error.status_reason();
        let _ = reporter.deployment_failed(reason).await;
        alarm_store.clear(UPDATE_IN_PROGRESS_ALARM);
        return Err(ClientError::Deployment(error));
    }

    alarm_store.clear(UPDATE_IN_PROGRESS_ALARM);
    let _ = event_tx.send(ClientEvent::FirmwareApplied).await;

    reporter.install_progress(100).await?;
    reporter.deployment_completed().await?;

    let reason = RebootReason::FirmwareApplied;
    if reboot_controller.is_enabled_for(reason) {
        reporter.rebooting().await?;
        reboot_controller.execute(reason).await?;
    } else {
        warn!("firmware applied but reboot is not enabled");
    }

    Ok(())
}
