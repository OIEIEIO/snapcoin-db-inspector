// =============================================================================
// File: src/scanner.rs
// Path: snapcoin-db-inspector/src/scanner.rs
// Version: v0.2.1-db-matched-tx-scanner-no-chrono.1
// Git Branch: dev
//
// Sentinel scanner matched to the current DB model.
//
// Current DB model:
// - default sled tree
// - key   = txid_base36
// - value = bincode(Vec<Option<TransactionOutput>>)
//
// This scanner:
// - iterates tx entries from sled
// - decodes Vec<Option<TransactionOutput>>
// - computes tx-level facts
// - evaluates tx anomaly rules
// =============================================================================

use anyhow::Result;
use sled::Db;
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

use snap_coin::core::transaction::{TransactionId, TransactionOutput};

use crate::events::{SentinelBlockContext, SentinelEvent, SentinelNetworkContext};
use crate::rules::{
    evaluate_all_tx_rules, format_atomic_8, ReceiverAggregateFact, TxOutputFact, TxRuleInput,
    TxRuleThresholds,
};

/// =============================================================================
/// Scan configuration
/// =============================================================================

#[derive(Debug, Clone)]
pub struct ScanConfig {
    pub chain: String,
    pub network_type: String,
    pub node_id: String,
    pub thresholds: TxRuleThresholds,
}

impl Default for ScanConfig {
    fn default() -> Self {
        Self {
            chain: "snapcoin".to_string(),
            network_type: "mainnet".to_string(),
            node_id: "snap-sentinel-01".to_string(),
            thresholds: TxRuleThresholds::default(),
        }
    }
}

/// =============================================================================
/// Scan outputs
/// =============================================================================

#[derive(Debug, Clone, Default)]
pub struct TxScanResult {
    pub txid: String,
    pub matched_events: Vec<SentinelEvent>,
}

#[derive(Debug, Clone, Default)]
pub struct RangeScanResult {
    pub start_height: u64,
    pub end_height: u64,
    pub scanned_blocks: u64,
    pub matched_events: Vec<SentinelEvent>,
}

/// =============================================================================
/// Compatibility type retained for sentinel.rs
/// =============================================================================

#[derive(Debug, Clone, Default)]
pub struct BlockScanResult {
    pub height: u64,
    pub matched_events: Vec<SentinelEvent>,
}

/// =============================================================================
/// Internal decoded facts
/// =============================================================================

#[derive(Debug, Clone)]
pub struct DecodedTxFacts {
    pub txid: String,
    pub output_count: usize,
    pub unspent_count: usize,
    pub spent_count: usize,
    pub tx_total_unspent_atomic: u64,
    pub largest_output_atomic: u64,
    pub largest_output_index: Option<u32>,
    pub largest_output_address: Option<String>,
    pub outputs: Vec<TxOutputFact>,
    pub receiver_aggregates: Vec<ReceiverAggregateFact>,
}

/// =============================================================================
/// Public scanner entrypoints
/// =============================================================================

/// Existing sentinel.rs expects a "block" scan signature. For now we interpret
/// the supplied height as an iteration index into valid tx entries.
pub fn scan_block(db: &Db, cfg: &ScanConfig, height: u64) -> Result<BlockScanResult> {
    let tx_entries = collect_valid_tx_entries(db)?;
    let idx = height as usize;

    if idx >= tx_entries.len() {
        return Ok(BlockScanResult {
            height,
            matched_events: Vec::new(),
        });
    }

    let (txid, outputs) = &tx_entries[idx];
    let facts = build_decoded_tx_facts(txid, outputs);
    let events = evaluate_decoded_facts(cfg, db, &facts);

    Ok(BlockScanResult {
        height,
        matched_events: events,
    })
}

pub fn scan_range(db: &Db, cfg: &ScanConfig, start_height: u64, end_height: u64) -> Result<RangeScanResult> {
    let tx_entries = collect_valid_tx_entries(db)?;
    let mut out = RangeScanResult {
        start_height,
        end_height,
        scanned_blocks: 0,
        matched_events: Vec::new(),
    };

    if end_height < start_height {
        return Ok(out);
    }

    let start = start_height as usize;
    let end = end_height as usize;

    for idx in start..=end {
        if idx >= tx_entries.len() {
            break;
        }

        let (txid, outputs) = &tx_entries[idx];
        let facts = build_decoded_tx_facts(txid, outputs);
        let events = evaluate_decoded_facts(cfg, db, &facts);

        out.scanned_blocks += 1;
        out.matched_events.extend(events);
    }

    Ok(out)
}

/// Full DB scan over tx entries.
/// This is the more natural scan mode for the current DB model.
pub fn scan_all_txs(db: &Db, cfg: &ScanConfig) -> Result<Vec<SentinelEvent>> {
    let tx_entries = collect_valid_tx_entries(db)?;
    let mut out = Vec::new();

    for (txid, outputs) in &tx_entries {
        let facts = build_decoded_tx_facts(txid, outputs);
        out.extend(evaluate_decoded_facts(cfg, db, &facts));
    }

    Ok(out)
}

/// =============================================================================
/// Core evaluation wiring
/// =============================================================================

fn evaluate_decoded_facts(cfg: &ScanConfig, db: &Db, facts: &DecodedTxFacts) -> Vec<SentinelEvent> {
    let network = SentinelNetworkContext {
        chain: cfg.chain.clone(),
        network_type: cfg.network_type.clone(),
        node_id: cfg.node_id.clone(),
        db_height: db.len() as u64,
        best_tip_hash: String::new(),
    };

    let block = SentinelBlockContext {
        height: 0,
        hash: String::new(),
        prev_hash: String::new(),
        timestamp: String::new(),
        tx_count: 1,
        coinbase_txid: facts.txid.clone(),
    };

    let input = TxRuleInput {
        detected_at: now_rfc3339_like(),
        network,
        block,
        txid: facts.txid.clone(),
        output_count: facts.output_count,
        unspent_count: facts.unspent_count,
        spent_count: facts.spent_count,
        tx_total_unspent_atomic: facts.tx_total_unspent_atomic,
        largest_output_atomic: facts.largest_output_atomic,
        largest_output_index: facts.largest_output_index,
        largest_output_address: facts.largest_output_address.clone(),
        outputs: facts.outputs.clone(),
        receiver_aggregates: facts.receiver_aggregates.clone(),
    };

    let rules_out = evaluate_all_tx_rules(&input, &cfg.thresholds);
    rules_out.matches.into_iter().map(|m| m.event).collect()
}

/// =============================================================================
/// DB decode helpers
/// =============================================================================

fn collect_valid_tx_entries(db: &Db) -> Result<Vec<(String, Vec<Option<TransactionOutput>>)>> {
    let mut out = Vec::new();

    for item in db.iter() {
        let (k, v) = item?;

        let txid_str = match String::from_utf8(k.to_vec()) {
            Ok(s) => s,
            Err(_) => continue,
        };

        let Some(_) = TransactionId::new_from_base36(&txid_str) else {
            continue;
        };

        let Ok((outputs, _)) = bincode::decode_from_slice::<Vec<Option<TransactionOutput>>, _>(
            &v,
            bincode::config::standard(),
        ) else {
            continue;
        };

        out.push((txid_str, outputs));
    }

    Ok(out)
}

fn build_decoded_tx_facts(txid: &str, outputs: &[Option<TransactionOutput>]) -> DecodedTxFacts {
    let mut tx_total_unspent_atomic: u64 = 0;
    let mut unspent_count: usize = 0;
    let mut spent_count: usize = 0;
    let mut largest_output_atomic: u64 = 0;
    let mut largest_output_index: Option<u32> = None;
    let mut largest_output_address: Option<String> = None;

    let mut out_facts: Vec<TxOutputFact> = Vec::new();
    let mut receiver_map: HashMap<String, (usize, u64)> = HashMap::new();

    for (i, o) in outputs.iter().enumerate() {
        match o {
            Some(out) => {
                unspent_count += 1;
                tx_total_unspent_atomic =
                    tx_total_unspent_atomic.saturating_add(out.amount);

                let addr = out.receiver.dump_base36();

                out_facts.push(TxOutputFact {
                    index: i as u32,
                    address: addr.clone(),
                    amount_atomic: out.amount,
                    amount_display: format_atomic_8(out.amount),
                });

                let entry = receiver_map.entry(addr.clone()).or_insert((0usize, 0u64));
                entry.0 += 1;
                entry.1 = entry.1.saturating_add(out.amount);

                if out.amount > largest_output_atomic {
                    largest_output_atomic = out.amount;
                    largest_output_index = Some(i as u32);
                    largest_output_address = Some(addr);
                }
            }
            None => {
                spent_count += 1;
            }
        }
    }

    let mut receiver_aggregates: Vec<ReceiverAggregateFact> = receiver_map
        .into_iter()
        .map(|(address, (output_count, total_atomic))| ReceiverAggregateFact {
            address,
            output_count,
            total_atomic,
            total_display: format_atomic_8(total_atomic),
        })
        .collect();

    receiver_aggregates.sort_by(|a, b| b.total_atomic.cmp(&a.total_atomic));

    DecodedTxFacts {
        txid: txid.to_string(),
        output_count: outputs.len(),
        unspent_count,
        spent_count,
        tx_total_unspent_atomic,
        largest_output_atomic,
        largest_output_index,
        largest_output_address,
        outputs: out_facts,
        receiver_aggregates,
    }
}

fn now_rfc3339_like() -> String {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(dur) => format!("unix:{}", dur.as_secs()),
        Err(_) => "unix:0".to_string(),
    }
}

// =============================================================================
// File: src/scanner.rs
// Path: snapcoin-db-inspector/src/scanner.rs
// =============================================================================