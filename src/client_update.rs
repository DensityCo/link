use crate::client::{ClientError, ClientEvent};
use crate::protocol::{
    progress_payload, ChannelBuilder, ProgressStage, ProtocolEvent, UpdateStatus,
};
use crate::update::{UpdateInfo, UpdateManager, UpdateRunEvent};
use futures_util::SinkExt;
use tokio::sync::mpsc;
use tracing::info;

pub async fn handle_update<S>(
    update_manager: UpdateManager,
    update_info: UpdateInfo,
    channel: &ChannelBuilder,
    write: &mut S,
    event_tx: &mpsc::Sender<ClientEvent>,
) -> Result<(), ClientError>
where
    S: SinkExt<tungstenite::Message> + Unpin,
    S::Error: std::fmt::Display,
{
    info!(
        uuid = %update_info.firmware_meta.uuid,
        version = %update_info.firmware_meta.version,
        "downloading firmware"
    );

    push(
        channel,
        write,
        ProtocolEvent::StatusUpdate,
        UpdateStatus::Received.payload(),
    )
    .await?;

    let (progress_tx, mut progress_rx) = mpsc::unbounded_channel::<UpdateRunEvent>();
    let update_handle = tokio::spawn(async move {
        update_manager
            .run(update_info, move |event| {
                let _ = progress_tx.send(event);
            })
            .await
    });

    push(
        channel,
        write,
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
            UpdateRunEvent::DownloadProgress(pct) => {
                let should_report = pct > last_reported_percent + 4 || pct == 100;

                if should_report {
                    last_reported_percent = pct;
                    let progress_msg = progress_payload(ProgressStage::Downloading, pct);
                    if let Err(error) =
                        push(channel, write, ProtocolEvent::FwupProgress, progress_msg).await
                    {
                        update_handle.abort();
                        return Err(error);
                    }
                }
            }
            UpdateRunEvent::FirmwareDownloaded(path) => {
                info!(path = %path.display(), "firmware downloaded");
                let _ = event_tx.send(ClientEvent::FirmwareDownloaded(path)).await;
            }
        }
    }

    match update_handle
        .await
        .map_err(|e| ClientError::Connection(format!("update task failed: {}", e)))?
    {
        Ok(_firmware_path) => {}
        Err(error) => {
            let reason = error.status_reason();
            let _ = push(
                channel,
                write,
                ProtocolEvent::StatusUpdate,
                UpdateStatus::Failed { reason }.payload(),
            )
            .await;
            return Err(ClientError::Update(error));
        }
    }

    let _ = event_tx.send(ClientEvent::FirmwareApplied).await;

    push(
        channel,
        write,
        ProtocolEvent::FwupProgress,
        progress_payload(ProgressStage::Updating, 100),
    )
    .await?;
    push(
        channel,
        write,
        ProtocolEvent::StatusUpdate,
        UpdateStatus::Completed.payload(),
    )
    .await?;

    Ok(())
}

async fn push<S>(
    channel: &ChannelBuilder,
    write: &mut S,
    event: ProtocolEvent,
    payload: serde_json::Value,
) -> Result<(), ClientError>
where
    S: SinkExt<tungstenite::Message> + Unpin,
    S::Error: std::fmt::Display,
{
    let msg = channel.push(event, payload);
    write
        .send(tungstenite::Message::Text(msg.to_json().into()))
        .await
        .map_err(|e| ClientError::WebSocket(e.to_string()))
}
