use crate::config::Config;
use crate::deployment::{
    download_firmware, progress_percent, Deployment, DeploymentError, FirmwareInstaller,
    FwupInstaller,
};
use std::path::PathBuf;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct DeploymentOptions {
    pub data_dir: PathBuf,
}

#[derive(Debug)]
pub enum DeploymentEvent {
    DownloadProgress(u8),
    FirmwareDownloaded(PathBuf),
}

#[derive(Clone)]
pub struct DeploymentManager {
    options: DeploymentOptions,
    installer: Arc<dyn FirmwareInstaller>,
}

impl DeploymentManager {
    pub fn new(options: DeploymentOptions, installer: Arc<dyn FirmwareInstaller>) -> Self {
        Self { options, installer }
    }

    pub fn with_installer<I>(options: DeploymentOptions, installer: I) -> Self
    where
        I: FirmwareInstaller + 'static,
    {
        Self::new(options, Arc::new(installer))
    }

    pub fn from_config(config: &Config) -> Self {
        let options = DeploymentOptions {
            data_dir: config.data_dir(),
        };
        let installer = Arc::new(FwupInstaller::new(
            config.fwup_devpath().to_string(),
            config.fwup_task().to_string(),
        ));

        Self::new(options, installer)
    }

    pub async fn run<F>(
        &self,
        deployment: Deployment,
        mut on_event: F,
    ) -> Result<PathBuf, DeploymentError>
    where
        F: FnMut(DeploymentEvent) + Send + 'static,
    {
        tokio::fs::create_dir_all(&self.options.data_dir)
            .await
            .map_err(DeploymentError::Io)?;

        let firmware_path =
            download_firmware(&deployment, &self.options.data_dir, |downloaded, total| {
                on_event(DeploymentEvent::DownloadProgress(progress_percent(
                    downloaded, total,
                )));
            })
            .await?;

        on_event(DeploymentEvent::FirmwareDownloaded(firmware_path.clone()));

        self.installer.apply(&firmware_path, &deployment).await?;

        Ok(firmware_path)
    }
}

pub async fn deploy_firmware<F>(
    deployment: Deployment,
    options: DeploymentOptions,
    installer: Arc<dyn FirmwareInstaller>,
    on_event: F,
) -> Result<PathBuf, DeploymentError>
where
    F: FnMut(DeploymentEvent) + Send + 'static,
{
    DeploymentManager::new(options, installer)
        .run(deployment, on_event)
        .await
}
