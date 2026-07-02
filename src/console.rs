use crate::config::ConsoleConfig;
use portable_pty::{native_pty_system, CommandBuilder, PtySize};
use std::fs::OpenOptions;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use thiserror::Error;
use tokio::sync::mpsc;
use tracing::{debug, warn};

#[derive(Debug, Error)]
pub enum ConsoleError {
    #[error("failed to open pty: {0}")]
    Open(String),
    #[error("failed to spawn console command: {0}")]
    Spawn(String),
    #[error("failed to read console output: {0}")]
    Read(String),
    #[error("failed to write console input: {0}")]
    Write(String),
    #[error("failed to resize console: {0}")]
    Resize(String),
    #[error("failed to stop console: {0}")]
    Stop(String),
    #[error("file transfer error: {0}")]
    File(String),
    #[error("invalid console file name: {0}")]
    InvalidFileName(String),
}

#[derive(Debug, Clone)]
pub struct ConsoleOptions {
    pub command: String,
    pub args: Vec<String>,
    pub rows: u16,
    pub cols: u16,
}

impl ConsoleOptions {
    pub fn from_config(config: &ConsoleConfig) -> Self {
        Self {
            command: config.command().to_string(),
            args: config.args().to_vec(),
            rows: config.rows(),
            cols: config.cols(),
        }
    }

    fn size(&self) -> PtySize {
        PtySize {
            rows: self.rows,
            cols: self.cols,
            pixel_width: 0,
            pixel_height: 0,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ConsoleOutput {
    pub data: String,
}

pub trait ConsoleBackend: Send + Sync {
    fn start(
        &self,
        output_tx: mpsc::UnboundedSender<ConsoleOutput>,
    ) -> Result<Box<dyn ConsoleSession>, ConsoleError>;
}

pub trait ConsoleSession: Send {
    fn write_input(&mut self, data: &str) -> Result<(), ConsoleError>;
    fn resize(&mut self, rows: u16, cols: u16) -> Result<(), ConsoleError>;
    fn stop(&mut self) -> Result<(), ConsoleError>;
}

#[derive(Debug)]
pub struct ConsoleFileReceiver {
    data_dir: PathBuf,
    active_path: Option<PathBuf>,
}

impl ConsoleFileReceiver {
    pub fn new(data_dir: PathBuf) -> Self {
        Self {
            data_dir,
            active_path: None,
        }
    }

    pub fn start(&mut self, filename: &str) -> Result<PathBuf, ConsoleError> {
        let path = safe_file_path(&self.data_dir, filename)?;
        std::fs::create_dir_all(&self.data_dir)
            .map_err(|error| ConsoleError::File(error.to_string()))?;
        std::fs::File::create(&path).map_err(|error| ConsoleError::File(error.to_string()))?;
        self.active_path = Some(path.clone());
        Ok(path)
    }

    pub fn append_base64(&mut self, data: &str) -> Result<(), ConsoleError> {
        use base64::Engine;

        let Some(path) = self.active_path.as_ref() else {
            return Err(ConsoleError::File(
                "no active console file upload".to_string(),
            ));
        };

        let chunk = base64::engine::general_purpose::STANDARD
            .decode(data)
            .map_err(|error| ConsoleError::File(error.to_string()))?;
        let mut file = OpenOptions::new()
            .append(true)
            .open(path)
            .map_err(|error| ConsoleError::File(error.to_string()))?;
        file.write_all(&chunk)
            .map_err(|error| ConsoleError::File(error.to_string()))
    }

    pub fn finish(&mut self) -> Option<PathBuf> {
        self.active_path.take()
    }
}

#[derive(Debug, Clone)]
pub struct PtyConsoleBackend {
    options: ConsoleOptions,
}

impl PtyConsoleBackend {
    pub fn new(options: ConsoleOptions) -> Self {
        Self { options }
    }

    pub fn from_config(config: &ConsoleConfig) -> Self {
        Self::new(ConsoleOptions::from_config(config))
    }
}

impl ConsoleBackend for PtyConsoleBackend {
    fn start(
        &self,
        output_tx: mpsc::UnboundedSender<ConsoleOutput>,
    ) -> Result<Box<dyn ConsoleSession>, ConsoleError> {
        let pty_system = native_pty_system();
        let pair = pty_system
            .openpty(self.options.size())
            .map_err(|error| ConsoleError::Open(error.to_string()))?;

        let mut command = CommandBuilder::new(&self.options.command);
        command.args(&self.options.args);

        let child = pair
            .slave
            .spawn_command(command)
            .map_err(|error| ConsoleError::Spawn(error.to_string()))?;

        let mut reader = pair
            .master
            .try_clone_reader()
            .map_err(|error| ConsoleError::Read(error.to_string()))?;

        let writer = pair
            .master
            .take_writer()
            .map_err(|error| ConsoleError::Write(error.to_string()))?;

        std::thread::spawn(move || {
            let mut buffer = [0_u8; 4096];
            loop {
                match reader.read(&mut buffer) {
                    Ok(0) => break,
                    Ok(count) => {
                        let data = String::from_utf8_lossy(&buffer[..count]).to_string();
                        if output_tx.send(ConsoleOutput { data }).is_err() {
                            break;
                        }
                    }
                    Err(error) => {
                        warn!(error = %error, "console output reader stopped");
                        break;
                    }
                }
            }
            debug!("console output reader exited");
        });

        Ok(Box::new(PtyConsoleSession {
            master: pair.master,
            child,
            writer,
            stopped: false,
        }))
    }
}

struct PtyConsoleSession {
    master: Box<dyn portable_pty::MasterPty + Send>,
    child: Box<dyn portable_pty::Child + Send + Sync>,
    writer: Box<dyn Write + Send>,
    stopped: bool,
}

impl ConsoleSession for PtyConsoleSession {
    fn write_input(&mut self, data: &str) -> Result<(), ConsoleError> {
        self.writer
            .write_all(data.as_bytes())
            .and_then(|_| self.writer.flush())
            .map_err(|error| ConsoleError::Write(error.to_string()))
    }

    fn resize(&mut self, rows: u16, cols: u16) -> Result<(), ConsoleError> {
        self.master
            .resize(PtySize {
                rows: rows.max(1),
                cols: cols.max(1),
                pixel_width: 0,
                pixel_height: 0,
            })
            .map_err(|error| ConsoleError::Resize(error.to_string()))
    }

    fn stop(&mut self) -> Result<(), ConsoleError> {
        if self.stopped {
            return Ok(());
        }

        self.stopped = true;
        self.child
            .kill()
            .map_err(|error| ConsoleError::Stop(error.to_string()))
    }
}

impl Drop for PtyConsoleSession {
    fn drop(&mut self) {
        let _ = self.stop();
    }
}

fn safe_file_path(data_dir: &Path, filename: &str) -> Result<PathBuf, ConsoleError> {
    let path = Path::new(filename);
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| ConsoleError::InvalidFileName(filename.to_string()))?;

    if name != filename || name == "." || name == ".." {
        return Err(ConsoleError::InvalidFileName(filename.to_string()));
    }

    Ok(data_dir.join(name))
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::time::{Duration, Instant};
    use tokio::sync::mpsc::error::TryRecvError;

    #[test]
    fn pty_backend_runs_shell_and_reads_output() {
        let backend = PtyConsoleBackend::new(ConsoleOptions {
            command: "/bin/sh".to_string(),
            args: Vec::new(),
            rows: 24,
            cols: 80,
        });
        let (tx, mut rx) = mpsc::unbounded_channel();
        let mut session = backend.start(tx).unwrap();

        session
            .write_input("echo link-console-ready\nexit\n")
            .unwrap();

        let deadline = Instant::now() + Duration::from_secs(3);
        let mut output = String::new();

        while Instant::now() < deadline {
            match rx.try_recv() {
                Ok(chunk) => {
                    output.push_str(&chunk.data);
                    if output.contains("link-console-ready") {
                        break;
                    }
                }
                Err(TryRecvError::Empty) => std::thread::sleep(Duration::from_millis(20)),
                Err(TryRecvError::Disconnected) => break,
            }
        }

        let _ = session.stop();
        assert!(output.contains("link-console-ready"), "{output}");
    }

    #[test]
    fn file_receiver_writes_base64_chunks_under_data_dir() {
        use base64::Engine;

        let dir = tempfile::tempdir().unwrap();
        let mut receiver = ConsoleFileReceiver::new(dir.path().to_path_buf());

        let path = receiver.start("hello.txt").unwrap();
        receiver
            .append_base64(&base64::engine::general_purpose::STANDARD.encode("hello "))
            .unwrap();
        receiver
            .append_base64(&base64::engine::general_purpose::STANDARD.encode("world"))
            .unwrap();

        assert_eq!(receiver.finish().as_deref(), Some(path.as_path()));
        assert_eq!(std::fs::read_to_string(path).unwrap(), "hello world");
    }

    #[test]
    fn file_receiver_rejects_path_traversal() {
        let dir = tempfile::tempdir().unwrap();
        let mut receiver = ConsoleFileReceiver::new(dir.path().to_path_buf());

        assert!(matches!(
            receiver.start("../escape.txt"),
            Err(ConsoleError::InvalidFileName(_))
        ));
        assert!(matches!(
            receiver.start("/tmp/escape.txt"),
            Err(ConsoleError::InvalidFileName(_))
        ));
    }
}
