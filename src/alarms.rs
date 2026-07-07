use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};

pub const DISCONNECTED_ALARM: &str = "link.disconnected";
pub const FIRMWARE_REVERTED_ALARM: &str = "link.firmware_reverted";
pub const UPDATE_IN_PROGRESS_ALARM: &str = "link.update_in_progress";

pub trait AlarmSource: Send + Sync {
    fn alarms(&self) -> BTreeMap<String, String>;
}

#[derive(Debug, Clone, Default)]
pub struct AlarmStore {
    inner: Arc<RwLock<BTreeMap<String, String>>>,
}

impl AlarmStore {
    pub fn set(&self, id: impl Into<String>, description: impl Into<String>) {
        self.inner
            .write()
            .expect("alarm store lock poisoned")
            .insert(id.into(), description.into());
    }

    pub fn clear(&self, id: &str) {
        self.inner
            .write()
            .expect("alarm store lock poisoned")
            .remove(id);
    }

    pub fn get(&self, id: &str) -> Option<String> {
        self.inner
            .read()
            .expect("alarm store lock poisoned")
            .get(id)
            .cloned()
    }

    pub fn list(&self) -> BTreeMap<String, String> {
        self.inner
            .read()
            .expect("alarm store lock poisoned")
            .clone()
    }

    pub fn snapshot(&self) -> BTreeMap<String, String> {
        self.list()
    }

    pub fn is_set(&self, id: &str) -> bool {
        self.inner
            .read()
            .expect("alarm store lock poisoned")
            .contains_key(id)
    }
}

impl AlarmSource for AlarmStore {
    fn alarms(&self) -> BTreeMap<String, String> {
        self.snapshot()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn alarm_store_sets_clears_and_snapshots() {
        let store = AlarmStore::default();

        store.set("link.test", "test alarm");
        assert!(store.is_set("link.test"));
        assert_eq!(store.get("link.test").as_deref(), Some("test alarm"));
        assert_eq!(
            store.list().get("link.test").map(String::as_str),
            Some("test alarm")
        );
        assert_eq!(store.snapshot(), store.list());

        store.clear("link.test");
        assert!(!store.is_set("link.test"));
        assert_eq!(store.get("link.test"), None);
        assert!(store.snapshot().is_empty());
    }
}
