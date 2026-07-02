pub mod download;
pub mod fwup;
pub mod installer;
pub mod runner;
pub mod types;

pub use download::{download_firmware, progress_percent};
pub use fwup::apply_firmware;
pub use installer::{FirmwareInstaller, FwupInstaller};
pub use runner::{download_and_apply, ApplyOptions, UpdateManager, UpdateRunEvent};
pub use types::{FirmwareMeta, UpdateInfo};

use thiserror::Error;

#[derive(Debug, Error)]
pub enum UpdateError {
    #[error("download failed: {0}")]
    Download(String),
    #[error("fwup failed: {0}")]
    Fwup(String),
    #[error("invalid update message: {0}")]
    InvalidMessage(String),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}

impl UpdateError {
    pub fn status_reason(&self) -> String {
        match self {
            UpdateError::Download(reason) => format!("Download failed: {}", reason),
            UpdateError::Fwup(reason) => format!("FWUP error: {}", reason),
            UpdateError::InvalidMessage(reason) => {
                format!("Invalid update message: {}", reason)
            }
            UpdateError::Io(error) => format!("Update IO error: {}", error),
        }
    }
}
