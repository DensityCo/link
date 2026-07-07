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
    public_keys: Vec<String>,
}

impl FwupInstaller {
    pub fn new(devpath: impl Into<String>, task: impl Into<String>) -> Self {
        Self {
            devpath: devpath.into(),
            task: task.into(),
            public_keys: Vec::new(),
        }
    }

    pub fn with_public_keys(mut self, public_keys: Vec<String>) -> Self {
        self.public_keys = public_keys;
        self
    }

    pub fn devpath(&self) -> &str {
        &self.devpath
    }

    pub fn task(&self) -> &str {
        &self.task
    }

    pub fn public_keys(&self) -> &[String] {
        &self.public_keys
    }
}

impl FirmwareInstaller for FwupInstaller {
    fn apply<'a>(
        &'a self,
        firmware_path: &'a Path,
        _deployment: &'a Deployment,
    ) -> BoxFuture<'a, Result<(), DeploymentError>> {
        Box::pin(async move {
            apply_firmware(firmware_path, &self.devpath, &self.task, &self.public_keys).await
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fwup_installer_stores_public_keys() {
        let installer = FwupInstaller::new("/dev/mmcblk0", "upgrade")
            .with_public_keys(vec!["key-1".to_string(), "key-2".to_string()]);

        assert_eq!(
            installer.public_keys(),
            &["key-1".to_string(), "key-2".to_string()]
        );
    }
}
