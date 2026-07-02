use chrono::{SecondsFormat, Utc};
use serde::Serialize;
use serde_json::{json, Value};
use std::collections::BTreeMap;

pub const HEALTH_EXTENSION_NAME: &str = "health";
pub const HEALTH_EXTENSION_VERSION: &str = "0.0.1";

pub type ValueMap = BTreeMap<String, Value>;

#[derive(Debug, Clone, Serialize, Default)]
pub struct HealthCheck {
    pub pass: bool,
    pub note: String,
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct HealthReport {
    pub timestamp: String,
    pub metadata: ValueMap,
    pub alarms: BTreeMap<String, String>,
    pub metrics: BTreeMap<String, f64>,
    pub checks: BTreeMap<String, HealthCheck>,
    pub connectivity: ValueMap,
}

pub trait HealthReporter: Send + Sync {
    fn report(&self) -> HealthReport;
}

#[derive(Debug, Clone, Default)]
pub struct SystemHealthReporter;

impl HealthReporter for SystemHealthReporter {
    fn report(&self) -> HealthReport {
        let mut system = sysinfo::System::new_all();
        system.refresh_all();

        let mut metadata = BTreeMap::new();
        insert_string(&mut metadata, "os_name", sysinfo::System::name());
        insert_string(&mut metadata, "os_version", sysinfo::System::os_version());
        insert_string(
            &mut metadata,
            "kernel_version",
            sysinfo::System::kernel_version(),
        );
        insert_string(&mut metadata, "host_name", sysinfo::System::host_name());

        let mut metrics = BTreeMap::new();
        let total_memory = system.total_memory() as f64;
        let used_memory = system.used_memory() as f64;
        metrics.insert("mem_size_mb".to_string(), bytes_to_mb(total_memory).round());
        metrics.insert("mem_used_mb".to_string(), bytes_to_mb(used_memory).round());
        metrics.insert(
            "mem_used_percent".to_string(),
            percent(used_memory, total_memory),
        );

        let cpus = system.cpus();
        if !cpus.is_empty() {
            let total_cpu: f32 = cpus.iter().map(|cpu| cpu.cpu_usage()).sum();
            metrics.insert(
                "cpu_usage_percent".to_string(),
                (total_cpu / cpus.len() as f32) as f64,
            );
        }

        let load = sysinfo::System::load_average();
        metrics.insert("load_1min".to_string(), round_to_places(load.one, 2));
        metrics.insert("load_5min".to_string(), round_to_places(load.five, 2));
        metrics.insert("load_15min".to_string(), round_to_places(load.fifteen, 2));

        let disks = sysinfo::Disks::new_with_refreshed_list();
        let total_disk: u64 = disks.iter().map(|disk| disk.total_space()).sum();
        let available_disk: u64 = disks.iter().map(|disk| disk.available_space()).sum();
        let used_disk = total_disk.saturating_sub(available_disk);
        metrics.insert(
            "disk_total_kb".to_string(),
            bytes_to_kb(total_disk as f64).round(),
        );
        metrics.insert(
            "disk_available_kb".to_string(),
            bytes_to_kb(available_disk as f64).round(),
        );
        metrics.insert(
            "disk_used_percentage".to_string(),
            percent(used_disk as f64, total_disk as f64),
        );

        HealthReport {
            timestamp: Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true),
            metadata,
            alarms: BTreeMap::new(),
            metrics,
            checks: BTreeMap::new(),
            connectivity: BTreeMap::new(),
        }
    }
}

pub fn available_extensions_payload() -> Value {
    json!({ HEALTH_EXTENSION_NAME: HEALTH_EXTENSION_VERSION })
}

pub fn extension_requested(payload: &Value, extension: &str) -> bool {
    let requested = payload.get("extensions").unwrap_or(payload);

    match requested {
        Value::String(value) => value == "all" || value == extension,
        Value::Array(values) => values.iter().any(|value| match value {
            Value::String(name) => name == extension,
            Value::Object(map) => map.contains_key(extension),
            _ => false,
        }),
        Value::Object(map) => map.contains_key(extension),
        _ => false,
    }
}

fn insert_string(map: &mut ValueMap, key: &str, value: Option<String>) {
    if let Some(value) = value {
        map.insert(key.to_string(), Value::String(value));
    }
}

fn percent(part: f64, total: f64) -> f64 {
    if total > 0.0 {
        ((part / total) * 100.0).min(100.0)
    } else {
        0.0
    }
}

fn bytes_to_kb(bytes: f64) -> f64 {
    bytes / 1_000.0
}

fn bytes_to_mb(bytes: f64) -> f64 {
    bytes / 1_000_000.0
}

fn round_to_places(value: f64, places: i32) -> f64 {
    let factor = 10_f64.powi(places);
    (value * factor).round() / factor
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extension_request_parser_accepts_common_shapes() {
        assert!(extension_requested(&json!({"extensions": "all"}), "health"));
        assert!(extension_requested(
            &json!({"extensions": ["health"]}),
            "health"
        ));
        assert!(extension_requested(&json!({"health": "0.0.1"}), "health"));
        assert!(!extension_requested(
            &json!({"extensions": ["geo"]}),
            "health"
        ));
    }

    #[test]
    fn default_report_contains_basic_metrics() {
        let report = SystemHealthReporter.report();

        assert!(!report.timestamp.is_empty());
        for key in [
            "cpu_usage_percent",
            "mem_size_mb",
            "mem_used_mb",
            "mem_used_percent",
            "load_1min",
            "load_5min",
            "load_15min",
            "disk_total_kb",
            "disk_available_kb",
            "disk_used_percentage",
        ] {
            assert!(report.metrics.contains_key(key), "missing {key}");
        }

        for key in [
            "memory_total_bytes",
            "memory_used_bytes",
            "memory_used_percent",
            "load_average_1m",
            "load_average_5m",
            "load_average_15m",
            "disk_total_bytes",
            "disk_used_bytes",
            "disk_used_percent",
        ] {
            assert!(!report.metrics.contains_key(key), "unexpected {key}");
        }
    }

    #[test]
    fn load_average_values_are_rounded_to_two_decimal_places() {
        assert_eq!(round_to_places(1.234, 2), 1.23);
        assert_eq!(round_to_places(1.235, 2), 1.24);
    }
}
