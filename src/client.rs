use crate::config::{Config, ConfigError};
use crate::console::{
    ConsoleBackend, ConsoleError, ConsoleFileReceiver, ConsoleOutput, ConsoleSession,
    PtyConsoleBackend,
};
use crate::deployment::{self, Deployment, DeploymentManager};
use crate::device::{DeviceInfo, DeviceInfoError, DeviceInfoProvider};
use crate::extensions::{
    available_extensions_payload, extension_requested, HealthReporter, SystemHealthReporter,
    HEALTH_EXTENSION_NAME,
};
use crate::outbound::{self, OutboundSender};
use crate::protocol::{ChannelBuilder, Message, ProtocolEvent};
use crate::transport;
use futures_util::{SinkExt, StreamExt};
use serde_json::json;
use std::future::Future;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::{Duration, Instant};
use tracing::{debug, error, info, warn};

#[derive(Debug, Error)]
pub enum ClientError {
    #[error("connection failed: {0}")]
    Connection(String),
    #[error("websocket error: {0}")]
    WebSocket(String),
    #[error("join rejected: {0}")]
    JoinRejected(String),
    #[error("config error: {0}")]
    Config(#[from] ConfigError),
    #[error("device info error: {0}")]
    DeviceInfo(#[from] DeviceInfoError),
    #[error("device info provider error: {0}")]
    DeviceInfoProvider(String),
    #[error("transport error: {0}")]
    Transport(#[from] transport::TransportError),
    #[error("deployment error: {0}")]
    Deployment(#[from] deployment::DeploymentError),
    #[error("console error: {0}")]
    Console(#[from] ConsoleError),
    #[error("channel closed")]
    ChannelClosed,
}

impl From<crate::outbound::OutboundError> for ClientError {
    fn from(error: crate::outbound::OutboundError) -> Self {
        ClientError::WebSocket(error.to_string())
    }
}

/// Events that the client can emit to the caller.
#[derive(Debug)]
pub enum ClientEvent {
    Connected,
    Joined,
    ExtensionsJoined,
    ConsoleJoined,
    ConsoleStarted,
    ConsoleStopped,
    HealthReported,
    DeploymentAvailable(Deployment),
    FirmwareDownloaded(std::path::PathBuf),
    FirmwareApplied,
    RebootRequested,
    Disconnected(String),
}

/// A device protocol client with platform-agnostic device metadata.
pub struct LinkClient {
    config: Config,
    device_info: DeviceInfo,
    deployment_manager: DeploymentManager,
    health_reporter: Arc<dyn HealthReporter>,
    console_backend: Option<Arc<dyn ConsoleBackend>>,
}

impl LinkClient {
    pub fn new(config: Config) -> Result<Self, ClientError> {
        let device_info = config.device_info()?;
        info!(
            serial = %device_info.serial_number.as_str(),
            "using configured device serial number"
        );
        Self::with_device_info(config, device_info)
    }

    pub fn with_device_info(config: Config, device_info: DeviceInfo) -> Result<Self, ClientError> {
        device_info.validate()?;
        let deployment_manager = DeploymentManager::from_config(&config);
        let console_backend = config
            .console
            .as_ref()
            .filter(|console| console.enabled())
            .map(|console| {
                Arc::new(PtyConsoleBackend::from_config(console)) as Arc<dyn ConsoleBackend>
            });
        Ok(Self {
            config,
            device_info,
            deployment_manager,
            health_reporter: Arc::new(SystemHealthReporter),
            console_backend,
        })
    }

    pub fn from_provider<P>(config: Config, provider: P) -> Result<Self, ClientError>
    where
        P: DeviceInfoProvider,
    {
        let device_info = provider
            .device_info()
            .map_err(|e| ClientError::DeviceInfoProvider(e.to_string()))?;
        Self::with_device_info(config, device_info)
    }

    pub fn set_device_info(&mut self, device_info: DeviceInfo) -> Result<(), ClientError> {
        device_info.validate()?;
        self.device_info = device_info;
        Ok(())
    }

    pub fn set_deployment_manager(&mut self, deployment_manager: DeploymentManager) {
        self.deployment_manager = deployment_manager;
    }

    pub fn with_deployment_manager(mut self, deployment_manager: DeploymentManager) -> Self {
        self.deployment_manager = deployment_manager;
        self
    }

    pub fn set_health_reporter<R>(&mut self, reporter: R)
    where
        R: HealthReporter + 'static,
    {
        self.health_reporter = Arc::new(reporter);
    }

    pub fn with_health_reporter<R>(mut self, reporter: R) -> Self
    where
        R: HealthReporter + 'static,
    {
        self.set_health_reporter(reporter);
        self
    }

    pub fn set_console_backend<B>(&mut self, backend: B)
    where
        B: ConsoleBackend + 'static,
    {
        self.console_backend = Some(Arc::new(backend));
        if self.device_info.console_version.is_none() {
            self.device_info.console_version = Some("2.0.0".to_string());
        }
    }

    pub fn disable_console(&mut self) {
        self.console_backend = None;
    }

    pub fn serial(&self) -> &str {
        &self.device_info.serial_number
    }

    /// Build the join payload with firmware metadata.
    pub fn join_payload(&self) -> serde_json::Value {
        self.device_info.join_payload()
    }

    /// Connect to the server and run the event loop.
    /// Sends events through the returned channel.
    pub async fn run(&self, event_tx: mpsc::Sender<ClientEvent>) -> Result<(), ClientError> {
        let ws_stream = transport::connect(&self.config, self.serial()).await?;
        let _ = event_tx.send(ClientEvent::Connected).await;

        let (mut write, mut read) = ws_stream.split();

        // Server's DeviceJSONSerializer rewrites "device" <-> "device:{id}" internally
        let topic = "device".to_string();
        let channel = ChannelBuilder::new(topic.clone());

        // Send join
        let join_msg = channel.join(self.join_payload());
        write
            .send(tungstenite::Message::Text(join_msg.to_json().into()))
            .await
            .map_err(|e| ClientError::WebSocket(e.to_string()))?;
        info!(topic = %topic, "sent channel join");

        // Wait for join reply
        let join_reply = Self::wait_for_reply(&mut read, &channel.join_ref).await?;
        if !join_reply.reply_ok() {
            let reason = join_reply
                .payload
                .get("response")
                .and_then(|r| r.get("reason"))
                .and_then(|r| r.as_str())
                .unwrap_or("unknown");
            return Err(ClientError::JoinRejected(reason.to_string()));
        }
        info!("joined device channel");
        let _ = event_tx.send(ClientEvent::Joined).await;

        let (outbound_tx, mut outbound_rx) = outbound::channel();
        let (connection_error_tx, mut connection_error_rx) = mpsc::unbounded_channel();
        let mut background_tasks = BackgroundTasks::new();
        let writer_error_tx = connection_error_tx.clone();
        background_tasks.spawn(async move {
            while let Some(outbound) = outbound_rx.recv().await {
                match write
                    .send(tungstenite::Message::Text(
                        outbound.message.to_json().into(),
                    ))
                    .await
                {
                    Ok(()) => {
                        let _ = outbound.result_tx.send(Ok(()));
                    }
                    Err(error) => {
                        let reason = error.to_string();
                        let _ = outbound.result_tx.send(Err(reason.clone()));
                        let _ = writer_error_tx.send(ClientError::WebSocket(reason));
                        break;
                    }
                }
            }
        });

        let mut console_channel = if self.console_backend.is_some() {
            let channel = ChannelBuilder::new("console".to_string());
            let join_msg = channel.join(self.join_payload());
            outbound_tx.send(join_msg).await?;
            info!("sent console channel join");
            Some(channel)
        } else {
            None
        };

        // Event loop: heartbeat + message handling
        let heartbeat_interval = Duration::from_secs(self.config.heartbeat_interval_secs());
        let mut next_heartbeat = Instant::now() + heartbeat_interval;
        let mut extensions_channel: Option<ChannelBuilder> = None;
        let mut health_attached = false;
        let (console_output_tx, mut console_output_rx) = mpsc::unbounded_channel();
        let mut console_session: Option<Box<dyn ConsoleSession>> = None;
        let mut console_deadline: Option<Instant> = None;
        let mut console_file_receiver = ConsoleFileReceiver::new(self.config.data_dir());

        loop {
            let console_timeout_at = console_deadline
                .unwrap_or_else(|| Instant::now() + Duration::from_secs(365 * 24 * 60 * 60));

            tokio::select! {
                msg = read.next() => {
                    match msg {
                        Some(Ok(tungstenite::Message::Text(text))) => {
                            debug!(message = %text, "received websocket text");
                            match Message::from_json(&text) {
                                Ok(msg) => {
                                    self.handle_message(
                                        msg,
                                        &channel,
                                        &mut console_channel,
                                        &mut extensions_channel,
                                        &mut health_attached,
                                        &mut console_session,
                                        &console_output_tx,
                                        &mut console_deadline,
                                        &mut console_file_receiver,
                                        &outbound_tx,
                                        &connection_error_tx,
                                        &mut background_tasks,
                                        &event_tx,
                                    )
                                    .await?;
                                }
                                Err(e) => {
                                    warn!(error = %e, "failed to parse message");
                                }
                            }
                        }
                        Some(Ok(tungstenite::Message::Close(_))) | None => {
                            if let Ok(error) = connection_error_rx.try_recv() {
                                let reason = error.to_string();
                                error!(error = %reason, "connection task failed before close");
                                let _ = event_tx.send(ClientEvent::Disconnected(reason)).await;
                                return Err(error);
                            }
                            info!("connection closed");
                            let _ = event_tx.send(ClientEvent::Disconnected("connection closed".to_string())).await;
                            return Ok(());
                        }
                        Some(Ok(_)) => {
                            // Ping/Pong/Binary - ignore
                        }
                        Some(Err(e)) => {
                            error!(error = %e, "websocket error");
                            let _ = event_tx.send(ClientEvent::Disconnected(e.to_string())).await;
                            return Err(ClientError::WebSocket(e.to_string()));
                        }
                    }
                }
                _ = tokio::time::sleep_until(next_heartbeat) => {
                    let hb = channel.heartbeat();
                    outbound_tx.send(hb).await?;
                    debug!("sent heartbeat");
                    next_heartbeat = Instant::now() + heartbeat_interval;
                }
                Some(output) = console_output_rx.recv() => {
                    self.forward_console_output(
                        output,
                        console_channel.as_ref(),
                        &mut console_deadline,
                        &outbound_tx,
                    )
                    .await?;
                }
                _ = tokio::time::sleep_until(console_timeout_at), if console_session.is_some() => {
                    self.stop_console_for_timeout(
                        &mut console_session,
                        console_channel.as_ref(),
                        &mut console_deadline,
                        &outbound_tx,
                        &event_tx,
                    )
                    .await?;
                }
                Some(error) = connection_error_rx.recv() => {
                    let reason = error.to_string();
                    error!(error = %reason, "connection task failed");
                    let _ = event_tx.send(ClientEvent::Disconnected(reason)).await;
                    return Err(error);
                }
            }
        }
    }

    async fn wait_for_reply<S>(read: &mut S, join_ref: &str) -> Result<Message, ClientError>
    where
        S: StreamExt<Item = Result<tungstenite::Message, tungstenite::Error>> + Unpin,
    {
        // Wait up to 30 seconds for a join reply
        let timeout = Duration::from_secs(30);
        let deadline = Instant::now() + timeout;

        loop {
            tokio::select! {
                msg = read.next() => {
                    match msg {
                        Some(Ok(tungstenite::Message::Text(text))) => {
                            if let Ok(msg) = Message::from_json(&text) {
                                if msg.is_reply() && msg.msg_ref.as_deref() == Some(join_ref) {
                                    return Ok(msg);
                                }
                            }
                        }
                        Some(Ok(_)) => continue,
                        Some(Err(e)) => return Err(ClientError::WebSocket(e.to_string())),
                        None => return Err(ClientError::ChannelClosed),
                    }
                }
                _ = tokio::time::sleep_until(deadline) => {
                    return Err(ClientError::Connection("join reply timeout".to_string()));
                }
            }
        }
    }

    async fn handle_message(
        &self,
        msg: Message,
        device_channel: &ChannelBuilder,
        console_channel: &mut Option<ChannelBuilder>,
        extensions_channel: &mut Option<ChannelBuilder>,
        health_attached: &mut bool,
        console_session: &mut Option<Box<dyn ConsoleSession>>,
        console_output_tx: &mpsc::UnboundedSender<ConsoleOutput>,
        console_deadline: &mut Option<Instant>,
        console_file_receiver: &mut ConsoleFileReceiver,
        outbound_tx: &OutboundSender,
        connection_error_tx: &mpsc::UnboundedSender<ClientError>,
        background_tasks: &mut BackgroundTasks,
        event_tx: &mpsc::Sender<ClientEvent>,
    ) -> Result<(), ClientError> {
        if msg.topic == "extensions" {
            self.handle_extensions_message(
                msg,
                extensions_channel,
                health_attached,
                outbound_tx,
                event_tx,
            )
            .await?;
            return Ok(());
        }

        if msg.topic == "console" {
            self.handle_console_message(
                msg,
                console_channel,
                console_session,
                console_output_tx,
                console_deadline,
                console_file_receiver,
                outbound_tx,
                event_tx,
            )
            .await?;
            return Ok(());
        }

        match msg.event() {
            Some(ProtocolEvent::ExtensionsGet) => {
                info!("received extensions request");
                let channel = ChannelBuilder::new("extensions".to_string());
                let join_msg = channel.join(available_extensions_payload());
                outbound_tx.send(join_msg).await?;
                *extensions_channel = Some(channel);
                info!("sent extensions channel join");
            }
            Some(ProtocolEvent::Update) => {
                info!("received deployment request");
                match Deployment::from_payload(&msg.payload) {
                    Ok(deployment) => {
                        let _ = event_tx
                            .send(ClientEvent::DeploymentAvailable(deployment.clone()))
                            .await;
                        let deployment_manager = self.deployment_manager.clone();
                        let deployment_channel = device_channel.clone();
                        let deployment_outbound_tx = outbound_tx.clone();
                        let deployment_event_tx = event_tx.clone();
                        let deployment_error_tx = connection_error_tx.clone();
                        background_tasks.spawn(async move {
                            if let Err(error) = crate::client_deployment::handle_deployment(
                                deployment_manager,
                                deployment,
                                deployment_channel,
                                deployment_outbound_tx,
                                deployment_event_tx,
                            )
                            .await
                            {
                                let _ = deployment_error_tx.send(error);
                            }
                        });
                    }
                    Err(e) => {
                        warn!(error = %e, "failed to parse deployment message");
                    }
                }
            }
            Some(ProtocolEvent::Reboot) => {
                info!("received reboot command");
                let _ = outbound_tx
                    .push(device_channel, ProtocolEvent::Rebooting, json!({}))
                    .await;
                let _ = event_tx.send(ClientEvent::RebootRequested).await;
            }
            Some(ProtocolEvent::PhxReply) => {
                debug!(
                    ref_id = ?msg.msg_ref,
                    status = ?msg.reply_status(),
                    "received reply"
                );
            }
            Some(ProtocolEvent::PhxError) => {
                warn!(topic = %msg.topic, "channel error");
            }
            Some(ProtocolEvent::PhxClose) => {
                info!(topic = %msg.topic, "channel closed by server");
                let _ = event_tx
                    .send(ClientEvent::Disconnected(
                        "channel closed by server".to_string(),
                    ))
                    .await;
                return Err(ClientError::ChannelClosed);
            }
            Some(event) => {
                debug!(event = event.as_str(), "unhandled event");
            }
            None => {
                debug!(event = msg.event, "unhandled event");
            }
        }
        Ok(())
    }

    async fn handle_console_message(
        &self,
        msg: Message,
        console_channel: &mut Option<ChannelBuilder>,
        console_session: &mut Option<Box<dyn ConsoleSession>>,
        console_output_tx: &mpsc::UnboundedSender<ConsoleOutput>,
        console_deadline: &mut Option<Instant>,
        console_file_receiver: &mut ConsoleFileReceiver,
        outbound_tx: &OutboundSender,
        event_tx: &mpsc::Sender<ClientEvent>,
    ) -> Result<(), ClientError> {
        if msg.is_reply() {
            debug!(
                ref_id = ?msg.msg_ref,
                status = ?msg.reply_status(),
                payload = %msg.payload,
                "received console reply"
            );

            if let Some(channel) = console_channel.as_ref() {
                if msg.msg_ref.as_deref() == Some(channel.join_ref.as_str()) && msg.reply_ok() {
                    info!("joined console channel");
                    let _ = event_tx.send(ClientEvent::ConsoleJoined).await;
                }
            }

            return Ok(());
        }

        let Some(channel) = console_channel.as_ref() else {
            warn!(event = %msg.event, "received console event before console channel joined");
            return Ok(());
        };

        match msg.event.as_str() {
            "dn" => {
                self.ensure_console_session(
                    console_session,
                    console_output_tx,
                    console_deadline,
                    event_tx,
                )
                .await?;

                if let Some(session) = console_session.as_mut() {
                    let data = msg
                        .payload
                        .get("data")
                        .and_then(|value| value.as_str())
                        .unwrap_or_default();
                    session.write_input(data)?;
                    self.reset_console_deadline(console_deadline);
                }
            }
            "window_size" => {
                self.ensure_console_session(
                    console_session,
                    console_output_tx,
                    console_deadline,
                    event_tx,
                )
                .await?;

                if let Some(session) = console_session.as_mut() {
                    let (rows, cols) = console_size_from_payload(&msg.payload);
                    session.resize(rows, cols)?;
                    self.reset_console_deadline(console_deadline);
                }
            }
            "restart" => {
                self.stop_console_session(console_session, console_deadline, event_tx)
                    .await?;
                self.push_console_text(channel, "\r*** Restarting shell ***\r", outbound_tx)
                    .await?;
                self.ensure_console_session(
                    console_session,
                    console_output_tx,
                    console_deadline,
                    event_tx,
                )
                .await?;
            }
            "file-data/start" => {
                let result = msg
                    .payload
                    .get("filename")
                    .and_then(|value| value.as_str())
                    .ok_or_else(|| ConsoleError::File("missing filename".to_string()))
                    .and_then(|filename| console_file_receiver.start(filename));

                match result {
                    Ok(path) => info!(path = %path.display(), "started console file upload"),
                    Err(error) => {
                        warn!(error = %error, "failed to start console file upload");
                        self.push_console_text(
                            channel,
                            &format!("\rconsole file upload failed: {error}\r\n"),
                            outbound_tx,
                        )
                        .await?;
                    }
                }
            }
            "file-data" => {
                let result = msg
                    .payload
                    .get("data")
                    .and_then(|value| value.as_str())
                    .ok_or_else(|| ConsoleError::File("missing data".to_string()))
                    .and_then(|data| console_file_receiver.append_base64(data));

                if let Err(error) = result {
                    warn!(error = %error, "failed to write console file upload chunk");
                    self.push_console_text(
                        channel,
                        &format!("\rconsole file upload failed: {error}\r\n"),
                        outbound_tx,
                    )
                    .await?;
                }
            }
            "file-data/stop" => {
                if let Some(path) = console_file_receiver.finish() {
                    info!(path = %path.display(), "finished console file upload");
                }
            }
            other => {
                debug!(event = other, "unhandled console event");
            }
        }

        Ok(())
    }

    async fn handle_extensions_message(
        &self,
        msg: Message,
        extensions_channel: &mut Option<ChannelBuilder>,
        health_attached: &mut bool,
        outbound_tx: &OutboundSender,
        event_tx: &mpsc::Sender<ClientEvent>,
    ) -> Result<(), ClientError> {
        if msg.is_reply() {
            debug!(
                ref_id = ?msg.msg_ref,
                status = ?msg.reply_status(),
                payload = %msg.payload,
                "received extensions reply"
            );

            if let Some(channel) = extensions_channel.as_ref() {
                if msg.msg_ref.as_deref() == Some(channel.join_ref.as_str()) && msg.reply_ok() {
                    info!("joined extensions channel");
                    let _ = event_tx.send(ClientEvent::ExtensionsJoined).await;
                    let response = msg.payload.get("response").unwrap_or(&msg.payload);
                    if extension_requested(response, HEALTH_EXTENSION_NAME) {
                        let attached = self
                            .attach_health(channel, health_attached, outbound_tx)
                            .await?;
                        if attached {
                            self.report_health(channel, outbound_tx, event_tx).await?;
                        }
                    } else {
                        info!(
                            "health extension not selected by server; health reports will not be sent"
                        );
                    }
                }
            }

            return Ok(());
        }

        let Some(channel) = extensions_channel.as_ref() else {
            warn!(event = %msg.event, "received extension event before extensions channel joined");
            return Ok(());
        };

        match msg.event.as_str() {
            "attach" => {
                if extension_requested(&msg.payload, HEALTH_EXTENSION_NAME) {
                    let attached = self
                        .attach_health(channel, health_attached, outbound_tx)
                        .await?;
                    if attached {
                        self.report_health(channel, outbound_tx, event_tx).await?;
                    }
                }
            }
            "detach" => {
                if extension_requested(&msg.payload, HEALTH_EXTENSION_NAME) {
                    self.detach_health(channel, health_attached, outbound_tx)
                        .await?;
                }
            }
            "health:check" => {
                if *health_attached {
                    self.report_health(channel, outbound_tx, event_tx).await?;
                } else {
                    warn!("health check requested before health extension attached");
                }
            }
            other => {
                debug!(event = other, "unhandled extensions event");
            }
        }

        Ok(())
    }

    async fn attach_health(
        &self,
        channel: &ChannelBuilder,
        health_attached: &mut bool,
        outbound_tx: &OutboundSender,
    ) -> Result<bool, ClientError> {
        if !*health_attached {
            outbound_tx
                .push_custom(channel, "health:attached", json!({}))
                .await?;
            *health_attached = true;
            info!("attached health extension");
            return Ok(true);
        }
        Ok(false)
    }

    async fn detach_health(
        &self,
        channel: &ChannelBuilder,
        health_attached: &mut bool,
        outbound_tx: &OutboundSender,
    ) -> Result<(), ClientError> {
        if *health_attached {
            outbound_tx
                .push_custom(channel, "health:detached", json!({}))
                .await?;
            *health_attached = false;
            info!("detached health extension");
        }
        Ok(())
    }

    async fn report_health(
        &self,
        channel: &ChannelBuilder,
        outbound_tx: &OutboundSender,
        event_tx: &mpsc::Sender<ClientEvent>,
    ) -> Result<(), ClientError> {
        let report = self.health_reporter.report();
        outbound_tx
            .push_custom(channel, "health:report", json!({ "value": report }))
            .await?;
        let _ = event_tx.send(ClientEvent::HealthReported).await;
        info!("reported health");
        Ok(())
    }

    async fn ensure_console_session(
        &self,
        console_session: &mut Option<Box<dyn ConsoleSession>>,
        console_output_tx: &mpsc::UnboundedSender<ConsoleOutput>,
        console_deadline: &mut Option<Instant>,
        event_tx: &mpsc::Sender<ClientEvent>,
    ) -> Result<(), ClientError> {
        if console_session.is_some() {
            return Ok(());
        }

        let Some(backend) = self.console_backend.as_ref() else {
            warn!("console message received but no console backend is configured");
            return Ok(());
        };

        let session = backend.start(console_output_tx.clone())?;
        *console_session = Some(session);
        self.reset_console_deadline(console_deadline);
        let _ = event_tx.send(ClientEvent::ConsoleStarted).await;
        info!("started console session");
        Ok(())
    }

    async fn stop_console_session(
        &self,
        console_session: &mut Option<Box<dyn ConsoleSession>>,
        console_deadline: &mut Option<Instant>,
        event_tx: &mpsc::Sender<ClientEvent>,
    ) -> Result<(), ClientError> {
        if let Some(mut session) = console_session.take() {
            if let Err(error) = session.stop() {
                warn!(error = %error, "failed to stop console session cleanly");
            }
            *console_deadline = None;
            let _ = event_tx.send(ClientEvent::ConsoleStopped).await;
            info!("stopped console session");
        }
        Ok(())
    }

    async fn stop_console_for_timeout(
        &self,
        console_session: &mut Option<Box<dyn ConsoleSession>>,
        console_channel: Option<&ChannelBuilder>,
        console_deadline: &mut Option<Instant>,
        outbound_tx: &OutboundSender,
        event_tx: &mpsc::Sender<ClientEvent>,
    ) -> Result<(), ClientError> {
        if let Some(channel) = console_channel {
            self.push_console_text(
                channel,
                "\r****************************************\r\n*   Session timeout due to inactivity  *\r\n*                                      *\r\n*   Press any key to continue...       *\r\n****************************************\r\n",
                outbound_tx,
            )
            .await?;
        }

        self.stop_console_session(console_session, console_deadline, event_tx)
            .await
    }

    async fn forward_console_output(
        &self,
        output: ConsoleOutput,
        console_channel: Option<&ChannelBuilder>,
        console_deadline: &mut Option<Instant>,
        outbound_tx: &OutboundSender,
    ) -> Result<(), ClientError> {
        let Some(channel) = console_channel else {
            return Ok(());
        };

        self.reset_console_deadline(console_deadline);
        self.push_console_text(channel, &output.data, outbound_tx)
            .await
    }

    async fn push_console_text(
        &self,
        channel: &ChannelBuilder,
        data: &str,
        outbound_tx: &OutboundSender,
    ) -> Result<(), ClientError> {
        if data.is_empty() {
            return Ok(());
        }

        outbound_tx
            .push_custom(channel, "up", json!({ "data": data }))
            .await?;
        Ok(())
    }

    fn reset_console_deadline(&self, console_deadline: &mut Option<Instant>) {
        let timeout_secs = self
            .config
            .console
            .as_ref()
            .map_or(5 * 60, |console| console.timeout_secs());
        *console_deadline = Some(Instant::now() + Duration::from_secs(timeout_secs));
    }
}

struct BackgroundTasks {
    handles: Vec<JoinHandle<()>>,
}

impl BackgroundTasks {
    fn new() -> Self {
        Self {
            handles: Vec::new(),
        }
    }

    fn spawn<F>(&mut self, future: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.handles.retain(|handle| !handle.is_finished());
        self.handles.push(tokio::spawn(future));
    }
}

impl Drop for BackgroundTasks {
    fn drop(&mut self) {
        for handle in &self.handles {
            handle.abort();
        }
    }
}

fn console_size_from_payload(payload: &serde_json::Value) -> (u16, u16) {
    let rows = payload
        .get("height")
        .or_else(|| payload.get("rows"))
        .and_then(|value| value.as_u64())
        .and_then(|value| u16::try_from(value).ok())
        .unwrap_or(24);

    let cols = payload
        .get("width")
        .or_else(|| payload.get("cols"))
        .and_then(|value| value.as_u64())
        .and_then(|value| u16::try_from(value).ok())
        .unwrap_or(80);

    (rows.max(1), cols.max(1))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::AuthConfig;
    use crate::device::{
        DeviceInfo, DeviceRuntimeState, FirmwareMetadata, StaticDeviceInfoProvider,
    };
    use std::collections::BTreeMap;

    fn test_config() -> Config {
        Config {
            host: "example.com".to_string(),
            auth: AuthConfig::SharedSecret {
                key: "test-key".to_string(),
                secret: "test-secret".to_string(),
            },
            serial_number: Some("test-device-001".to_string()),
            fwup_devpath: None,
            fwup_task: None,
            firmware: Some(FirmwareMetadata {
                uuid: "fw-uuid-123".to_string(),
                version: "1.0.0".to_string(),
                platform: "rpi4".to_string(),
                architecture: "arm".to_string(),
                product: "test-product".to_string(),
            }),
            heartbeat_interval_secs: None,
            data_dir: None,
            device_api_version: None,
            console_version: None,
            fwup_version: None,
            currently_downloading_uuid: None,
            firmware_validated: None,
            firmware_auto_revert_detected: None,
            join_params: None,
            console: None,
        }
    }

    #[test]
    fn client_creation() {
        let client = LinkClient::new(test_config()).unwrap();
        assert_eq!(client.serial(), "test-device-001");
    }

    #[test]
    fn join_payload_contains_metadata() {
        let client = LinkClient::new(test_config()).unwrap();
        let payload = client.join_payload();
        assert_eq!(payload["nerves_fw_uuid"], "fw-uuid-123");
        assert_eq!(payload["nerves_fw_version"], "1.0.0");
        assert_eq!(payload["nerves_fw_platform"], "rpi4");
        assert_eq!(payload["nerves_fw_architecture"], "arm");
        assert_eq!(payload["nerves_fw_product"], "test-product");
        assert_eq!(payload["device_api_version"], "2.3.0");
    }

    #[test]
    fn join_payload_custom_api_version() {
        let mut config = test_config();
        config.device_api_version = Some("2.0.0".to_string());
        let client = LinkClient::new(config).unwrap();
        let payload = client.join_payload();
        assert_eq!(payload["device_api_version"], "2.0.0");
    }

    #[test]
    fn client_can_use_device_info_provider() {
        let mut config = test_config();
        config.serial_number = None;
        let firmware = config.firmware.take().unwrap();

        let info = DeviceInfo {
            serial_number: "provider-device-001".to_string(),
            firmware,
            device_api_version: "2.3.0".to_string(),
            fwup_version: Some("1.12.0".to_string()),
            console_version: Some("2.0.0".to_string()),
            runtime_state: DeviceRuntimeState {
                firmware_validated: Some(true),
                ..DeviceRuntimeState::default()
            },
            extra_join_params: BTreeMap::new(),
        };

        let client =
            LinkClient::from_provider(config, StaticDeviceInfoProvider::new(info)).unwrap();
        let payload = client.join_payload();

        assert_eq!(client.serial(), "provider-device-001");
        assert_eq!(payload["fwup_version"], "1.12.0");
        assert_eq!(payload["meta"]["firmware_validated"], true);
    }

    #[test]
    fn client_can_use_runtime_device_info_directly() {
        let mut config = test_config();
        config.serial_number = None;
        config.firmware = None;

        let info = DeviceInfo {
            serial_number: "runtime-device-001".to_string(),
            firmware: FirmwareMetadata {
                uuid: "runtime-fw".to_string(),
                version: "2.0.0".to_string(),
                platform: "x86_64".to_string(),
                architecture: "x86_64".to_string(),
                product: "runtime-product".to_string(),
            },
            device_api_version: "2.3.0".to_string(),
            fwup_version: Some("1.13.0".to_string()),
            console_version: None,
            runtime_state: DeviceRuntimeState {
                currently_downloading_uuid: Some("download-123".to_string()),
                firmware_validated: Some(false),
                firmware_auto_revert_detected: Some(true),
            },
            extra_join_params: BTreeMap::new(),
        };

        let client = LinkClient::with_device_info(config, info).unwrap();
        let payload = client.join_payload();

        assert_eq!(client.serial(), "runtime-device-001");
        assert_eq!(payload["nerves_fw_uuid"], "runtime-fw");
        assert_eq!(payload["fwup_version"], "1.13.0");
        assert_eq!(payload["currently_downloading_uuid"], "download-123");
        assert_eq!(payload["meta"]["firmware_auto_revert_detected"], true);
    }

    #[test]
    fn client_can_update_runtime_device_info() {
        let mut client = LinkClient::new(test_config()).unwrap();

        let info = DeviceInfo {
            serial_number: "updated-runtime-device".to_string(),
            firmware: FirmwareMetadata {
                uuid: "updated-fw".to_string(),
                version: "3.0.0".to_string(),
                platform: "rpi5".to_string(),
                architecture: "aarch64".to_string(),
                product: "updated-product".to_string(),
            },
            device_api_version: "2.3.0".to_string(),
            fwup_version: None,
            console_version: None,
            runtime_state: DeviceRuntimeState::default(),
            extra_join_params: BTreeMap::new(),
        };

        client.set_device_info(info).unwrap();

        let payload = client.join_payload();
        assert_eq!(client.serial(), "updated-runtime-device");
        assert_eq!(payload["nerves_fw_uuid"], "updated-fw");
        assert_eq!(payload["nerves_fw_platform"], "rpi5");
    }

    #[test]
    fn console_size_accepts_console_and_local_shell_shapes() {
        assert_eq!(
            console_size_from_payload(&json!({"height": 40, "width": 120})),
            (40, 120)
        );
        assert_eq!(
            console_size_from_payload(&json!({"rows": 30, "cols": 100})),
            (30, 100)
        );
        assert_eq!(console_size_from_payload(&json!({})), (24, 80));
    }
}
