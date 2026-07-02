use crate::update::{apply_firmware, UpdateError, UpdateInfo};
use futures_util::future::BoxFuture;
use std::path::Path;

pub trait FirmwareInstaller: Send + Sync {
    fn apply<'a>(
        &'a self,
        firmware_path: &'a Path,
        update_info: &'a UpdateInfo,
    ) -> BoxFuture<'a, Result<(), UpdateError>>;
}

#[derive(Debug, Clone)]
pub struct FwupInstaller {
    devpath: String,
    task: String,
}

impl FwupInstaller {
    pub fn new(devpath: impl Into<String>, task: impl Into<String>) -> Self {
        Self {
            devpath: devpath.into(),
            task: task.into(),
        }
    }

    pub fn devpath(&self) -> &str {
        &self.devpath
    }

    pub fn task(&self) -> &str {
        &self.task
    }
}

impl FirmwareInstaller for FwupInstaller {
    fn apply<'a>(
        &'a self,
        firmware_path: &'a Path,
        _update_info: &'a UpdateInfo,
    ) -> BoxFuture<'a, Result<(), UpdateError>> {
        Box::pin(async move { apply_firmware(firmware_path, &self.devpath, &self.task).await })
    }
}
