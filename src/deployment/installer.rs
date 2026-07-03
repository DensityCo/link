use crate::deployment::{apply_firmware, Deployment, DeploymentError};
use futures_util::future::BoxFuture;
use std::path::Path;

pub trait FirmwareInstaller: Send + Sync {
    fn apply<'a>(
        &'a self,
        firmware_path: &'a Path,
        deployment: &'a Deployment,
    ) -> BoxFuture<'a, Result<(), DeploymentError>>;
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
        _deployment: &'a Deployment,
    ) -> BoxFuture<'a, Result<(), DeploymentError>> {
        Box::pin(async move { apply_firmware(firmware_path, &self.devpath, &self.task).await })
    }
}
