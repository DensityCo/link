pub mod auth;
pub mod client;
mod client_deployment;
pub mod config;
mod connection;
pub mod console;
mod console_handler;
pub mod deployment;
mod deployment_supervisor;
pub mod device;
pub mod extensions;
mod extensions_handler;
mod message_router;
mod outbound;
pub mod protocol;
pub mod runner;
mod task_set;
mod tls;
pub mod transport;

pub use client::{ClientEvent, LinkClient};
pub use config::{AuthConfig, Config, ConfigError};
pub use console::{ConsoleBackend, ConsoleOutput, ConsoleSession, PtyConsoleBackend};
pub use deployment::{
    Deployment, DeploymentError, DeploymentManager, DeploymentOptions, FirmwareInstaller,
    FirmwareMeta, FwupInstaller,
};
pub use device::{
    DeviceInfo, DeviceInfoError, DeviceInfoProvider, DeviceRuntimeState, FirmwareMetadata,
    StaticDeviceInfoProvider,
};
pub use extensions::{HealthReport, HealthReporter, SystemHealthReporter};
pub use runner::{backoff_delay, LinkRunner, RunnerOptions};
