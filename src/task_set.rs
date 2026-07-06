use std::future::Future;
use tokio::task::JoinHandle;

pub(crate) struct TaskSet {
    handles: Vec<JoinHandle<()>>,
}

impl TaskSet {
    pub(crate) fn new() -> Self {
        Self {
            handles: Vec::new(),
        }
    }

    pub(crate) fn spawn<F>(&mut self, future: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.handles.retain(|handle| !handle.is_finished());
        self.handles.push(tokio::spawn(future));
    }
}

impl Drop for TaskSet {
    fn drop(&mut self) {
        for handle in &self.handles {
            handle.abort();
        }
    }
}
