pub mod auth;
pub mod client;
mod client_update;
pub mod config;
pub mod console;
pub mod device;
pub mod extensions;
pub mod protocol;
pub mod runner;
pub mod transport;
pub mod update;

pub use client::{ClientEvent, LinkClient};
pub use config::{AuthConfig, Config, ConfigError};
pub use console::{ConsoleBackend, ConsoleOutput, ConsoleSession, PtyConsoleBackend};
pub use device::{
    DeviceInfo, DeviceInfoError, DeviceInfoProvider, DeviceRuntimeState, FirmwareMetadata,
    StaticDeviceInfoProvider,
};
pub use extensions::{HealthReport, HealthReporter, SystemHealthReporter};
pub use runner::{backoff_delay, LinkRunner, RunnerOptions};
pub use update::{
    ApplyOptions, FirmwareInstaller, FirmwareMeta, FwupInstaller, UpdateError, UpdateInfo,
    UpdateManager,
};
