use crate::client::{ClientError, ClientEvent};
use crate::deployment::{Deployment, DeploymentEvent, DeploymentManager};
use crate::outbound::OutboundSender;
use crate::protocol::{
    progress_payload, ChannelBuilder, ProgressStage, ProtocolEvent, UpdateStatus,
};
use tokio::sync::mpsc;
use tracing::info;

pub async fn handle_deployment(
    deployment_manager: DeploymentManager,
    deployment: Deployment,
    channel: ChannelBuilder,
    outbound: OutboundSender,
    event_tx: mpsc::Sender<ClientEvent>,
) -> Result<(), ClientError> {
    info!(
        uuid = %deployment.firmware_meta.uuid,
        version = %deployment.firmware_meta.version,
        "downloading firmware"
    );

    push(
        &outbound,
        &channel,
        ProtocolEvent::StatusUpdate,
        UpdateStatus::Received.payload(),
    )
    .await?;

    let (progress_tx, mut progress_rx) = mpsc::unbounded_channel::<DeploymentEvent>();
    let deployment_handle = tokio::spawn(async move {
        deployment_manager
            .run(deployment, move |event| {
                let _ = progress_tx.send(event);
            })
            .await
    });

    push(
        &outbound,
        &channel,
        ProtocolEvent::StatusUpdate,
        UpdateStatus::Started {
            downloader_network_interface: None,
        }
        .payload(),
    )
    .await?;

    let mut last_reported_percent: u8 = 0;
    while let Some(event) = progress_rx.recv().await {
        match event {
            DeploymentEvent::DownloadProgress(pct) => {
                let should_report = pct > last_reported_percent + 4 || pct == 100;

                if should_report {
                    last_reported_percent = pct;
                    let progress_msg = progress_payload(ProgressStage::Downloading, pct);
                    if let Err(error) = push(
                        &outbound,
                        &channel,
                        ProtocolEvent::FwupProgress,
                        progress_msg,
                    )
                    .await
                    {
                        deployment_handle.abort();
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

    match deployment_handle
        .await
        .map_err(|e| ClientError::Connection(format!("deployment task failed: {}", e)))?
    {
        Ok(_firmware_path) => {}
        Err(error) => {
            let reason = error.status_reason();
            let _ = push(
                &outbound,
                &channel,
                ProtocolEvent::StatusUpdate,
                UpdateStatus::Failed { reason }.payload(),
            )
            .await;
            return Err(ClientError::Deployment(error));
        }
    }

    let _ = event_tx.send(ClientEvent::FirmwareApplied).await;

    push(
        &outbound,
        &channel,
        ProtocolEvent::FwupProgress,
        progress_payload(ProgressStage::Updating, 100),
    )
    .await?;
    push(
        &outbound,
        &channel,
        ProtocolEvent::StatusUpdate,
        UpdateStatus::Completed.payload(),
    )
    .await?;

    Ok(())
}

async fn push(
    outbound: &OutboundSender,
    channel: &ChannelBuilder,
    event: ProtocolEvent,
    payload: serde_json::Value,
) -> Result<(), ClientError> {
    outbound.push(channel, event, payload).await?;
    Ok(())
}
