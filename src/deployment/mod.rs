mod download;
mod fwup;
mod installer;
mod runner;
mod types;

pub use download::{download_firmware, progress_percent};
pub use fwup::apply_firmware;
pub use installer::{FirmwareInstaller, FwupInstaller};
pub use runner::{DeploymentEvent, DeploymentManager, DeploymentOptions};
pub use types::{Deployment, FirmwareMeta};

use thiserror::Error;

#[derive(Debug, Error)]
pub enum DeploymentError {
    #[error("download failed: {0}")]
    Download(String),
    #[error("fwup failed: {0}")]
    Fwup(String),
    #[error("invalid deployment message: {0}")]
    InvalidMessage(String),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}

impl DeploymentError {
    pub fn status_reason(&self) -> String {
        match self {
            DeploymentError::Download(reason) => format!("Download failed: {}", reason),
            DeploymentError::Fwup(reason) => format!("FWUP error: {}", reason),
            DeploymentError::InvalidMessage(reason) => {
                format!("Invalid deployment message: {}", reason)
            }
            DeploymentError::Io(error) => format!("Deployment IO error: {}", error),
        }
    }
}
