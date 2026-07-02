use crate::config::Config;
use crate::update::{
    download_firmware, progress_percent, FirmwareInstaller, FwupInstaller, UpdateError, UpdateInfo,
};
use std::path::PathBuf;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct ApplyOptions {
    pub data_dir: PathBuf,
}

#[derive(Debug)]
pub enum UpdateRunEvent {
    DownloadProgress(u8),
    FirmwareDownloaded(PathBuf),
}

#[derive(Clone)]
pub struct UpdateManager {
    options: ApplyOptions,
    installer: Arc<dyn FirmwareInstaller>,
}

impl UpdateManager {
    pub fn new(options: ApplyOptions, installer: Arc<dyn FirmwareInstaller>) -> Self {
        Self { options, installer }
    }

    pub fn with_installer<I>(options: ApplyOptions, installer: I) -> Self
    where
        I: FirmwareInstaller + 'static,
    {
        Self::new(options, Arc::new(installer))
    }

    pub fn from_config(config: &Config) -> Self {
        let options = ApplyOptions {
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
        update_info: UpdateInfo,
        mut on_event: F,
    ) -> Result<PathBuf, UpdateError>
    where
        F: FnMut(UpdateRunEvent) + Send + 'static,
    {
        tokio::fs::create_dir_all(&self.options.data_dir)
            .await
            .map_err(UpdateError::Io)?;

        let firmware_path =
            download_firmware(&update_info, &self.options.data_dir, |downloaded, total| {
                on_event(UpdateRunEvent::DownloadProgress(progress_percent(
                    downloaded, total,
                )));
            })
            .await?;

        on_event(UpdateRunEvent::FirmwareDownloaded(firmware_path.clone()));

        self.installer.apply(&firmware_path, &update_info).await?;

        Ok(firmware_path)
    }
}

pub async fn download_and_apply<F>(
    update_info: UpdateInfo,
    options: ApplyOptions,
    installer: Arc<dyn FirmwareInstaller>,
    on_event: F,
) -> Result<PathBuf, UpdateError>
where
    F: FnMut(UpdateRunEvent) + Send + 'static,
{
    UpdateManager::new(options, installer)
        .run(update_info, on_event)
        .await
}
