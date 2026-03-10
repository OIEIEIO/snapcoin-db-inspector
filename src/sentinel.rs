// =============================================================================
// File: src/sentinel.rs
// Path: snapcoin-db-inspector/src/sentinel.rs
// Version: v0.2.1-db-matched-sentinel-orchestrator-footer-fix.1
// Git Branch: dev
//
// Sentinel orchestrator matched to the current DB model.
//
// Current DB model:
// - default sled tree
// - key   = txid_base36
// - value = bincode(Vec<Option<TransactionOutput>>)
//
// Responsibilities:
// - drive tx-entry anomaly scanning
// - emit anomaly events
// - support manual indexed scans, range scans, full scans, and watch mode
//
// Notes:
// - The current DB is tx-entry centric, not block centric.
// - The inherited "block scan" API is retained for compatibility with the
//   existing project shape, but internally it scans indexed tx entries.
// =============================================================================

use anyhow::Result;
use serde_json::to_string_pretty;
use sled::Db;
use tokio::time::{sleep, Duration};

use crate::events::SentinelEvent;
use crate::scanner::{scan_all_txs, scan_block, scan_range, ScanConfig};

/// =============================================================================
/// Sentinel configuration
/// =============================================================================

#[derive(Debug, Clone)]
pub struct SentinelConfig {
    pub chain: String,
    pub network_type: String,
    pub node_id: String,
    pub poll_seconds: u64,
}

impl Default for SentinelConfig {
    fn default() -> Self {
        Self {
            chain: "snapcoin".to_string(),
            network_type: "mainnet".to_string(),
            node_id: "snap-sentinel-01".to_string(),
            poll_seconds: 5,
        }
    }
}

impl SentinelConfig {
    pub fn to_scan_config(&self) -> ScanConfig {
        ScanConfig {
            chain: self.chain.clone(),
            network_type: self.network_type.clone(),
            node_id: self.node_id.clone(),
            thresholds: Default::default(),
        }
    }
}

/// =============================================================================
/// Event output helpers
/// =============================================================================

pub fn emit_event(event: &SentinelEvent) {
    match to_string_pretty(event) {
        Ok(json) => {
            println!("=== SENTINEL ALERT ===");
            println!("{json}");
        }
        Err(e) => {
            println!("sentinel: failed to serialize event: {e}");
        }
    }
}

pub fn emit_events(events: &[SentinelEvent]) {
    for event in events {
        emit_event(event);
    }
}

/// =============================================================================
/// Indexed single-entry scan
/// =============================================================================

/// Compatibility path: scans one indexed tx entry from the DB.
pub fn run_index_scan(db: &Db, cfg: &SentinelConfig, index: u64) -> Result<()> {
    let scan_cfg = cfg.to_scan_config();
    let result = scan_block(db, &scan_cfg, index)?;

    if result.matched_events.is_empty() {
        println!("sentinel: indexed entry {index} clean");
    } else {
        println!(
            "sentinel: indexed entry {index} produced {} alert(s)",
            result.matched_events.len()
        );
        emit_events(&result.matched_events);
    }

    Ok(())
}

/// =============================================================================
/// Indexed range scan
/// =============================================================================

/// Compatibility path: scans a range of indexed tx entries from the DB.
pub fn run_index_range_scan(db: &Db, cfg: &SentinelConfig, start: u64, end: u64) -> Result<()> {
    let scan_cfg = cfg.to_scan_config();

    println!(
        "sentinel: scanning indexed entries {}..{} ({})",
        start,
        end,
        end.saturating_sub(start) + 1
    );

    let result = scan_range(db, &scan_cfg, start, end)?;

    println!("sentinel: scanned {} indexed entries", result.scanned_blocks);

    if result.matched_events.is_empty() {
        println!("sentinel: no anomalies detected");
    } else {
        println!(
            "sentinel: {} anomaly event(s) detected",
            result.matched_events.len()
        );
        emit_events(&result.matched_events);
    }

    Ok(())
}

/// =============================================================================
/// Full DB tx scan
/// =============================================================================

/// Natural scan mode for the current DB model.
pub fn run_full_tx_scan(db: &Db, cfg: &SentinelConfig) -> Result<()> {
    let scan_cfg = cfg.to_scan_config();

    println!("sentinel: starting full tx-entry scan");
    let events = scan_all_txs(db, &scan_cfg)?;

    if events.is_empty() {
        println!("sentinel: no anomalies detected in full tx scan");
    } else {
        println!("sentinel: {} anomaly event(s) detected", events.len());
        emit_events(&events);
    }

    Ok(())
}

/// =============================================================================
/// Continuous monitoring loop
/// =============================================================================

/// Watch mode currently re-scans the full DB on each poll.
///
/// This is simple and safe for now.
/// Later we can optimize to incremental scanning with a persistent cursor.
pub async fn run_watch_loop(db: Db, cfg: SentinelConfig) -> Result<()> {
    let scan_cfg = cfg.to_scan_config();

    println!(
        "sentinel: watch mode starting (poll={}s)",
        cfg.poll_seconds
    );

    loop {
        let events = scan_all_txs(&db, &scan_cfg)?;

        if events.is_empty() {
            println!("sentinel: watch poll complete, no anomalies detected");
        } else {
            println!(
                "sentinel: watch poll complete, {} anomaly event(s) detected",
                events.len()
            );
            emit_events(&events);
        }

        sleep(Duration::from_secs(cfg.poll_seconds)).await;
    }
}

/// =============================================================================
/// Quick diagnostic helper
/// =============================================================================

pub fn print_banner() {
    println!("---------------------------------------------");
    println!(" Snap Sentinel");
    println!(" Tx / UTXO Anomaly Monitor");
    println!("---------------------------------------------");
}

// =============================================================================
// File: src/sentinel.rs
// Path: snapcoin-db-inspector/src/sentinel.rs
// =============================================================================