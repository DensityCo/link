use crate::client::ClientError;
use crate::protocol::{
    progress_payload, DeviceChannel, ProgressStage, ProtocolEvent, UpdateStatus,
};
use crate::transport::TransportSender;
use serde_json::json;

#[derive(Clone)]
pub(crate) struct DeviceReporter {
    channel: DeviceChannel,
    transport_tx: TransportSender,
}

impl DeviceReporter {
    pub(crate) fn new(channel: DeviceChannel, transport_tx: TransportSender) -> Self {
        Self {
            channel,
            transport_tx,
        }
    }

    pub(crate) async fn rebooting(&self) -> Result<(), ClientError> {
        self.transport_tx.send(self.channel.rebooting()).await?;
        Ok(())
    }

    pub(crate) async fn deployment_received(&self) -> Result<(), ClientError> {
        self.status(UpdateStatus::Received).await
    }

    pub(crate) async fn deployment_started(
        &self,
        downloader_network_interface: Option<String>,
    ) -> Result<(), ClientError> {
        self.status(UpdateStatus::Started {
            downloader_network_interface,
        })
        .await
    }

    pub(crate) async fn download_progress(&self, percent: u8) -> Result<(), ClientError> {
        self.progress(ProgressStage::Downloading, percent).await
    }

    pub(crate) async fn install_progress(&self, percent: u8) -> Result<(), ClientError> {
        self.progress(ProgressStage::Updating, percent).await
    }

    pub(crate) async fn deployment_completed(&self) -> Result<(), ClientError> {
        self.status(UpdateStatus::Completed).await
    }

    pub(crate) async fn deployment_failed(&self, reason: String) -> Result<(), ClientError> {
        self.status(UpdateStatus::Failed { reason }).await
    }

    pub(crate) async fn script_completed(
        &self,
        script_ref: &str,
        output: &str,
        return_value: &str,
    ) -> Result<(), ClientError> {
        self.transport_tx
            .send(self.channel.push(
                ProtocolEvent::ScriptsRun,
                json!({
                    "ref": script_ref,
                    "result": "completed",
                    "output": output,
                    "return": return_value,
                }),
            ))
            .await?;
        Ok(())
    }

    pub(crate) async fn script_failed(
        &self,
        script_ref: &str,
        reason: &str,
        output: &str,
    ) -> Result<(), ClientError> {
        self.transport_tx
            .send(self.channel.push(
                ProtocolEvent::ScriptsRun,
                json!({
                    "ref": script_ref,
                    "result": "error",
                    "reason": reason,
                    "output": output,
                    "return": "",
                }),
            ))
            .await?;
        Ok(())
    }

    async fn status(&self, status: UpdateStatus) -> Result<(), ClientError> {
        self.transport_tx
            .send(
                self.channel
                    .push(ProtocolEvent::StatusUpdate, status.payload()),
            )
            .await?;
        Ok(())
    }

    async fn progress(&self, stage: ProgressStage, percent: u8) -> Result<(), ClientError> {
        self.transport_tx
            .send(self.channel.push(
                ProtocolEvent::FwupProgress,
                progress_payload(stage, percent),
            ))
            .await?;
        Ok(())
    }
}
