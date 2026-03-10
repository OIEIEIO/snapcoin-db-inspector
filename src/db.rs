// =============================================================================
// src/db.rs
// snapcoin-db-inspector/src/db.rs
// v0.4.0-wallet-intelligence.1
// Pure sled query helpers, CLI subcommands, AppState definition.
// No HTTP or WebSocket dependencies.
//
// Change (v0.4.0 ... .1):
// - ADD WalletIntelligence struct and wallet_intelligence() DB scan function
//   Returns global intelligence metrics derived from a single full DB scan:
//     - most_active_wallet (highest tx count)
//     - most_fragmented_wallet (highest unspent output count)
//     - fastest_growing_wallet (largest delta: current - baseline unspent)
//     - highest_spend_rate_wallet (highest spent/total ratio)
//     - largest_single_output (amount + txid + receiver)
//   Baseline balance = sum of unspent at first-seen iteration index per address.
//   Iteration order is sled lexicographic key order (used as sequence proxy).
// - ADD WalletStats struct and wallet_stats() for per-address summary
//   Returns: tx_count, unspent_count, spent_count, total_unspent_atomic,
//            baseline_unspent_atomic, first_seen_txid, last_seen_txid,
//            largest_output_amount, avg_output_amount, fragmentation_rating
// =============================================================================

use anyhow::{anyhow, Context, Result};
use clap::Subcommand;
use serde::Serialize;
use sled::IVec;
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use snap_coin::core::transaction::{TransactionId, TransactionOutput};
use snap_coin::crypto::keys::Public;

// ---------------------------------------------------------------------------
// Shared state
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct AppState {
    pub db: Arc<sled::Db>,
}

// ---------------------------------------------------------------------------
// CLI subcommands
// ---------------------------------------------------------------------------

#[derive(Subcommand, Debug)]
pub enum Cmd {
    /// Scan keys/values in a tree (optionally with a key prefix)
    Scan {
        #[arg(long)]
        tree: String,
        #[arg(long, default_value_t = 50)]
        limit: usize,
        /// Prefix filter: "hex:...." or "utf8:...."
        #[arg(long)]
        prefix: Option<String>,
        #[arg(long, default_value_t = false)]
        show_values: bool,
    },

    /// Get one key from a tree
    Get {
        #[arg(long)]
        tree: String,
        /// Key: "hex:...." or "utf8:...."
        #[arg(long)]
        key: String,
        #[arg(long, default_value_t = true)]
        show_value: bool,
    },

    /// Decode UTXO entry (txid -> Vec<Option<TransactionOutput>>)
    DecodeUtxos {
        #[arg(long)]
        txid: String,
    },

    /// Find txids containing at least one output for an address
    UtxosForAddress {
        #[arg(long)]
        address: String,
        #[arg(long, default_value_t = 100)]
        limit: usize,
    },

    /// Confirmed balance for an address (sum of unspent outputs)
    Balance {
        #[arg(long)]
        address: String,
    },

    /// Top receivers by total unspent
    TopReceivers {
        #[arg(long, default_value_t = 25)]
        limit: usize,
    },

    /// Global metrics (scan default tree)
    Globals,

    /// Global wallet intelligence metrics (single full scan)
    WalletIntelligence,

    /// Per-wallet summary stats for a given address
    WalletStats {
        #[arg(long)]
        address: String,
    },

    // ---------------------------
    // Default-tree inspection
    // ---------------------------

    /// Inspect DEFAULT tree (__sled__default): prefix histogram + sample keys
    InspectDefault {
        #[arg(long, default_value_t = 5000)]
        sample: usize,
        #[arg(long, default_value_t = 25)]
        top: usize,
        #[arg(long, default_value_t = 20)]
        examples: usize,
    },

    /// Prefix histogram for DEFAULT tree (__sled__default)
    PrefixStats {
        #[arg(long, default_value_t = 5000)]
        sample: usize,
        #[arg(long, default_value_t = 25)]
        top: usize,
    },

    /// Print first N keys from DEFAULT tree (__sled__default) as utf8/hex preview
    PreviewKeys {
        #[arg(long, default_value_t = 50)]
        limit: usize,
    },
}

// ---------------------------------------------------------------------------
// Global metrics (DEFAULT tree only)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize)]
pub struct GlobalMetrics {
    pub tx_entries_scanned: u64,
    pub outputs_total: u64,
    pub outputs_unspent: u64,
    pub outputs_spent: u64,
    pub wallets_with_unspent: u64,
    pub utxo_total_unspent_atomic: u64,
}

/// Scan the DEFAULT tree only (db.iter()) and compute high-level totals.
///
/// Storage assumption: txid_base36 -> bincode(Vec<Option<TxOut>>).
pub fn global_metrics(db: &sled::Db) -> Result<GlobalMetrics> {
    let mut tx_entries_scanned: u64 = 0;
    let mut outputs_total: u64 = 0;
    let mut outputs_unspent: u64 = 0;
    let mut outputs_spent: u64 = 0;
    let mut utxo_total_unspent_atomic: u64 = 0;

    let mut receivers_with_unspent: HashSet<String> = HashSet::new();

    for item in db.iter() {
        let (k, v) = item?;

        let txid_str = match String::from_utf8(k.to_vec()) {
            Ok(s) => s,
            Err(_) => continue,
        };

        let Some(_) = TransactionId::new_from_base36(&txid_str) else { continue };

        let Ok((outputs, _)) = bincode::decode_from_slice::<Vec<Option<TransactionOutput>>, _>(
            &v,
            bincode::config::standard(),
        ) else {
            continue;
        };

        tx_entries_scanned += 1;
        outputs_total += outputs.len() as u64;

        for o in outputs.into_iter() {
            match o {
                Some(out) => {
                    outputs_unspent += 1;
                    utxo_total_unspent_atomic =
                        utxo_total_unspent_atomic.saturating_add(out.amount);
                    receivers_with_unspent.insert(out.receiver.dump_base36());
                }
                None => {
                    outputs_spent += 1;
                }
            }
        }
    }

    Ok(GlobalMetrics {
        tx_entries_scanned,
        outputs_total,
        outputs_unspent,
        outputs_spent,
        wallets_with_unspent: receivers_with_unspent.len() as u64,
        utxo_total_unspent_atomic,
    })
}

// ---------------------------------------------------------------------------
// Per-address accumulator (used internally by intelligence scan)
// ---------------------------------------------------------------------------

#[derive(Debug, Default)]
struct AddrAccum {
    tx_count: u64,
    unspent_count: u64,
    spent_count: u64,
    total_unspent_atomic: u64,
    largest_output: u64,
    // Baseline = unspent sum at first-seen iteration index
    baseline_unspent_atomic: u64,
    baseline_set: bool,
    first_seen_txid: String,
    last_seen_txid: String,
    // Sequence indices (iteration order as proxy for time)
    first_seen_index: u64,
    last_seen_index: u64,
}

// ---------------------------------------------------------------------------
// WalletIntelligence — global derived metrics
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize)]
pub struct WalletIntelligence {
    /// Wallet with the highest number of transactions
    pub most_active_address: String,
    pub most_active_tx_count: u64,

    /// Wallet with the most unspent outputs (fragmented)
    pub most_fragmented_address: String,
    pub most_fragmented_unspent_count: u64,

    /// Wallet with the largest positive delta (current - baseline unspent)
    pub fastest_growing_address: String,
    pub fastest_growing_delta_atomic: i64,
    pub fastest_growing_delta_snap: String,

    /// Wallet with highest spent/total output ratio (min 5 total outputs to qualify)
    pub highest_spend_rate_address: String,
    pub highest_spend_rate_pct: f64,
    pub highest_spend_rate_spent: u64,
    pub highest_spend_rate_total: u64,

    /// Single largest unspent output in entire DB
    pub largest_output_amount_atomic: u64,
    pub largest_output_amount_snap: String,
    pub largest_output_txid: String,
    pub largest_output_receiver: String,

    /// Total wallets scanned
    pub total_wallets_scanned: u64,
    /// Total tx entries scanned
    pub total_tx_scanned: u64,
}

/// Single full DB scan that builds per-address accumulators and derives
/// global intelligence metrics. Uses sled iteration order as sequence proxy.
pub fn wallet_intelligence(db: &sled::Db) -> Result<WalletIntelligence> {
    // addr_base36 -> accumulator
    let mut accum: HashMap<String, AddrAccum> = HashMap::new();

    let mut largest_output_amount: u64 = 0;
    let mut largest_output_txid = String::new();
    let mut largest_output_receiver = String::new();

    let mut tx_index: u64 = 0;

    for item in db.iter() {
        let (k, v) = item?;

        let txid_str = match String::from_utf8(k.to_vec()) {
            Ok(s) => s,
            Err(_) => continue,
        };

        let Some(_) = TransactionId::new_from_base36(&txid_str) else { continue };

        let Ok((outputs, _)) = bincode::decode_from_slice::<Vec<Option<TransactionOutput>>, _>(
            &v,
            bincode::config::standard(),
        ) else {
            continue;
        };

        // Collect unique receivers in this tx to update their accumulators
        let mut seen_in_tx: HashSet<String> = HashSet::new();

        for o in outputs.iter() {
            match o {
                Some(out) => {
                    let addr = out.receiver.dump_base36();
                    seen_in_tx.insert(addr.clone());

                    let entry = accum.entry(addr.clone()).or_default();
                    entry.unspent_count += 1;
                    entry.total_unspent_atomic =
                        entry.total_unspent_atomic.saturating_add(out.amount);

                    if out.amount > entry.largest_output {
                        entry.largest_output = out.amount;
                    }

                    // Track global largest output
                    if out.amount > largest_output_amount {
                        largest_output_amount = out.amount;
                        largest_output_txid = txid_str.clone();
                        largest_output_receiver = addr.clone();
                    }
                }
                None => {
                    // Spent slot — we don't know the receiver; count at tx level below
                }
            }
        }

        // Count spent outputs at tx level (no receiver on spent)
        let spent_in_tx = outputs.iter().filter(|o| o.is_none()).count() as u64;

        // Update tx-level stats for all addresses seen in this tx
        for addr in &seen_in_tx {
            let entry = accum.entry(addr.clone()).or_default();

            entry.tx_count += 1;
            entry.spent_count += spent_in_tx;
            entry.last_seen_txid = txid_str.clone();
            entry.last_seen_index = tx_index;

            if !entry.baseline_set {
                // First time we see this address — snapshot current unspent as baseline
                entry.first_seen_txid = txid_str.clone();
                entry.first_seen_index = tx_index;
                entry.baseline_unspent_atomic = entry.total_unspent_atomic;
                entry.baseline_set = true;
            }
        }

        tx_index += 1;
    }

    let total_wallets = accum.len() as u64;
    let total_tx = tx_index;

    // ── Derive intelligence metrics ──

    let mut most_active_address = String::new();
    let mut most_active_tx_count = 0u64;

    let mut most_fragmented_address = String::new();
    let mut most_fragmented_unspent_count = 0u64;

    let mut fastest_growing_address = String::new();
    let mut fastest_growing_delta: i64 = i64::MIN;

    let mut highest_spend_rate_address = String::new();
    let mut highest_spend_rate_pct = 0f64;
    let mut highest_spend_rate_spent = 0u64;
    let mut highest_spend_rate_total = 0u64;

    for (addr, a) in &accum {
        // Most active
        if a.tx_count > most_active_tx_count {
            most_active_tx_count = a.tx_count;
            most_active_address = addr.clone();
        }

        // Most fragmented (most unspent outputs)
        if a.unspent_count > most_fragmented_unspent_count {
            most_fragmented_unspent_count = a.unspent_count;
            most_fragmented_address = addr.clone();
        }

        // Fastest growing (current unspent - baseline unspent)
        let delta = (a.total_unspent_atomic as i64)
            .saturating_sub(a.baseline_unspent_atomic as i64);
        if delta > fastest_growing_delta {
            fastest_growing_delta = delta;
            fastest_growing_address = addr.clone();
        }

        // Highest spend rate (min 5 total outputs to qualify)
        let total_outputs = a.unspent_count + a.spent_count;
        if total_outputs >= 5 {
            let rate = a.spent_count as f64 / total_outputs as f64 * 100.0;
            if rate > highest_spend_rate_pct {
                highest_spend_rate_pct = rate;
                highest_spend_rate_address = addr.clone();
                highest_spend_rate_spent = a.spent_count;
                highest_spend_rate_total = total_outputs;
            }
        }
    }

    let fastest_growing_delta_snap = atomic_to_snap_string(fastest_growing_delta.unsigned_abs());
    let fastest_growing_delta_snap = if fastest_growing_delta < 0 {
        format!("-{}", fastest_growing_delta_snap)
    } else {
        format!("+{}", fastest_growing_delta_snap)
    };

    Ok(WalletIntelligence {
        most_active_address,
        most_active_tx_count,
        most_fragmented_address,
        most_fragmented_unspent_count,
        fastest_growing_address,
        fastest_growing_delta_atomic: fastest_growing_delta,
        fastest_growing_delta_snap,
        highest_spend_rate_address,
        highest_spend_rate_pct: (highest_spend_rate_pct * 100.0).round() / 100.0,
        highest_spend_rate_spent,
        highest_spend_rate_total,
        largest_output_amount_atomic: largest_output_amount,
        largest_output_amount_snap: atomic_to_snap_string(largest_output_amount),
        largest_output_txid,
        largest_output_receiver,
        total_wallets_scanned: total_wallets,
        total_tx_scanned: total_tx,
    })
}

// ---------------------------------------------------------------------------
// WalletStats — per-address summary
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize)]
pub struct WalletStats {
    pub address: String,
    pub tx_count: u64,
    pub unspent_count: u64,
    pub spent_count: u64,
    pub total_unspent_atomic: u64,
    pub total_unspent_snap: String,
    pub baseline_unspent_atomic: u64,
    pub baseline_unspent_snap: String,
    pub delta_atomic: i64,
    pub delta_snap: String,
    pub trend: String, // "rising" | "falling" | "stable"
    pub spend_rate_pct: f64,
    pub largest_output_atomic: u64,
    pub largest_output_snap: String,
    pub avg_output_atomic: u64,
    pub avg_output_snap: String,
    pub fragmentation: String, // "LOW" | "MEDIUM" | "HIGH"
    pub first_seen_txid: String,
    pub last_seen_txid: String,
    pub first_seen_index: u64,
    pub last_seen_index: u64,
}

/// Per-address summary stats. Single scan filtered to the target address.
/// Uses iteration order as sequence proxy for first/last seen and baseline.
pub fn wallet_stats(db: &sled::Db, address: &Public) -> Result<WalletStats> {
    let addr_str = address.dump_base36();

    let mut tx_count: u64 = 0;
    let mut unspent_count: u64 = 0;
    let mut spent_count: u64 = 0;
    let mut total_unspent_atomic: u64 = 0;
    let mut largest_output: u64 = 0;
    let mut baseline_unspent_atomic: u64 = 0;
    let mut baseline_set = false;
    let mut first_seen_txid = String::new();
    let mut last_seen_txid = String::new();
    let mut first_seen_index: u64 = 0;
    let mut last_seen_index: u64 = 0;

    let mut tx_index: u64 = 0;

    for item in db.iter() {
        let (k, v) = item?;

        let txid_str = match String::from_utf8(k.to_vec()) {
            Ok(s) => s,
            Err(_) => continue,
        };

        let Some(_) = TransactionId::new_from_base36(&txid_str) else { continue };

        let Ok((outputs, _)) = bincode::decode_from_slice::<Vec<Option<TransactionOutput>>, _>(
            &v,
            bincode::config::standard(),
        ) else {
            tx_index += 1;
            continue;
        };

        let addr_in_tx = outputs
            .iter()
            .any(|o| o.is_some_and(|o| o.receiver == *address));

        if addr_in_tx {
            let spent_in_tx = outputs.iter().filter(|o| o.is_none()).count() as u64;
            spent_count += spent_in_tx;

            for o in outputs.iter() {
                if let Some(out) = o {
                    if out.receiver == *address {
                        unspent_count += 1;
                        total_unspent_atomic =
                            total_unspent_atomic.saturating_add(out.amount);
                        if out.amount > largest_output {
                            largest_output = out.amount;
                        }
                    }
                }
            }

            tx_count += 1;
            last_seen_txid = txid_str.clone();
            last_seen_index = tx_index;

            if !baseline_set {
                first_seen_txid = txid_str.clone();
                first_seen_index = tx_index;
                // Snapshot running unspent total as baseline at first appearance
                baseline_unspent_atomic = total_unspent_atomic;
                baseline_set = true;
            }
        }

        tx_index += 1;
    }

    let avg_output_atomic = if unspent_count > 0 {
        total_unspent_atomic / unspent_count
    } else {
        0
    };

    let delta = (total_unspent_atomic as i64).saturating_sub(baseline_unspent_atomic as i64);

    let trend = if delta > 0 {
        "rising".to_string()
    } else if delta < 0 {
        "falling".to_string()
    } else {
        "stable".to_string()
    };

    let delta_snap = {
        let s = atomic_to_snap_string(delta.unsigned_abs());
        if delta < 0 { format!("-{s}") } else { format!("+{s}") }
    };

    let total_outputs = unspent_count + spent_count;
    let spend_rate_pct = if total_outputs > 0 {
        (spent_count as f64 / total_outputs as f64 * 10000.0).round() / 100.0
    } else {
        0.0
    };

    // Fragmentation: LOW < 5 unspent, MEDIUM 5–20, HIGH > 20
    let fragmentation = if unspent_count <= 4 {
        "LOW".to_string()
    } else if unspent_count <= 20 {
        "MEDIUM".to_string()
    } else {
        "HIGH".to_string()
    };

    Ok(WalletStats {
        address: addr_str,
        tx_count,
        unspent_count,
        spent_count,
        total_unspent_atomic,
        total_unspent_snap: atomic_to_snap_string(total_unspent_atomic),
        baseline_unspent_atomic,
        baseline_unspent_snap: atomic_to_snap_string(baseline_unspent_atomic),
        delta_atomic: delta,
        delta_snap,
        trend,
        spend_rate_pct,
        largest_output_atomic: largest_output,
        largest_output_snap: atomic_to_snap_string(largest_output),
        avg_output_atomic,
        avg_output_snap: atomic_to_snap_string(avg_output_atomic),
        fragmentation,
        first_seen_txid,
        last_seen_txid,
        first_seen_index,
        last_seen_index,
    })
}

// ---------------------------------------------------------------------------
// Helper: atomic units -> SNAP string (8 decimal places, trimmed)
// ---------------------------------------------------------------------------

fn atomic_to_snap_string(atomic: u64) -> String {
    const DIVISOR: u64 = 100_000_000;
    let whole = atomic / DIVISOR;
    let frac = atomic % DIVISOR;
    let frac_str = format!("{:08}", frac);
    let trimmed = frac_str.trim_end_matches('0');
    let trimmed = if trimmed.is_empty() { "00" } else { trimmed };
    // Pad to minimum 2 decimal places
    let trimmed = if trimmed.len() < 2 {
        &frac_str[..2]
    } else {
        trimmed
    };
    format!("{}.{}", whole.to_string()
        .as_bytes()
        .rchunks(3)
        .rev()
        .map(std::str::from_utf8)
        .collect::<Result<Vec<_>, _>>()
        .unwrap_or_default()
        .join(","),
        trimmed
    )
}

// ---------------------------------------------------------------------------
// Default-tree inspection
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize)]
pub struct PrefixRow {
    pub bucket: String,
    pub count: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct KeyExample {
    pub key_utf8: Option<String>,
    pub key_hex: String,
    pub value_len: usize,
    pub bucket: String,
    pub looks_like_txid: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct DefaultTreeInspect {
    pub tree: String,
    pub len: u64,
    pub sample: usize,
    pub sampled: usize,
    pub looks_like_txid_keys: u64,
    pub non_utf8_keys: u64,
    pub top_prefixes: Vec<PrefixRow>,
    pub examples: Vec<KeyExample>,
}

#[derive(Debug, Clone, Serialize)]
pub struct PrefixStatsOut {
    pub tree: String,
    pub len: u64,
    pub sample: usize,
    pub sampled: usize,
    pub looks_like_txid_keys: u64,
    pub non_utf8_keys: u64,
    pub top_prefixes: Vec<PrefixRow>,
}

#[derive(Debug, Clone, Serialize)]
pub struct PreviewKeyRow {
    pub key_utf8: Option<String>,
    pub key_hex: String,
    pub value_len: usize,
    pub bucket: String,
    pub looks_like_txid: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct PreviewKeysOut {
    pub tree: String,
    pub len: u64,
    pub limit: usize,
    pub keys: Vec<PreviewKeyRow>,
}

fn bucket_key(k: &[u8]) -> (String, Option<String>, bool, bool) {
    let utf8 = String::from_utf8(k.to_vec()).ok();
    if let Some(s) = utf8 {
        let printable = s.chars().all(|c| !c.is_control());
        if printable {
            let looks_like_txid = TransactionId::new_from_base36(&s).is_some();
            if let Some((pfx, _rest)) = s.split_once(':') {
                let p = pfx.trim();
                let p = if p.is_empty() { "utf8:<empty_prefix>" } else { p };
                let p = if p.len() > 48 { &p[..48] } else { p };
                return (format!("pfx:{p}"), Some(s), looks_like_txid, false);
            }
            if looks_like_txid {
                return ("txid".to_string(), Some(s), true, false);
            }
            return ("utf8".to_string(), Some(s), false, false);
        }
        let b = bucket_hex_prefix(k);
        return (b, None, false, true);
    }
    let b = bucket_hex_prefix(k);
    (b, None, false, true)
}

fn bucket_hex_prefix(k: &[u8]) -> String {
    if k.is_empty() { return "hex:<empty>".to_string(); }
    if k.len() == 1 { return format!("hex:{:02x}", k[0]); }
    format!("hex:{:02x} {:02x}", k[0], k[1])
}

pub fn default_prefix_stats(
    db: &sled::Db,
    sample: usize,
    top: usize,
) -> Result<(usize, Vec<PrefixRow>, u64, u64)> {
    let mut buckets: std::collections::HashMap<String, u64> = std::collections::HashMap::new();
    let mut sampled: usize = 0;
    let mut looks_like_txid_keys: u64 = 0;
    let mut non_utf8_keys: u64 = 0;

    for item in db.iter().take(sample) {
        let (k, _v) = item?;
        sampled += 1;
        let (bucket, _utf8, looks_like_txid, is_non_utf8) = bucket_key(&k);
        *buckets.entry(bucket).or_insert(0) += 1;
        if looks_like_txid { looks_like_txid_keys += 1; }
        if is_non_utf8 { non_utf8_keys += 1; }
    }

    let mut rows: Vec<PrefixRow> = buckets
        .into_iter()
        .map(|(bucket, count)| PrefixRow { bucket, count })
        .collect();
    rows.sort_by(|a, b| b.count.cmp(&a.count));
    rows.truncate(top);
    Ok((sampled, rows, looks_like_txid_keys, non_utf8_keys))
}

pub fn inspect_default_tree(
    db: &sled::Db,
    sample: usize,
    top: usize,
    examples: usize,
) -> Result<DefaultTreeInspect> {
    let len: u64 = db.len().try_into().unwrap();
    let (sampled, top_prefixes, looks_like_txid_keys, non_utf8_keys) =
        default_prefix_stats(db, sample, top)?;

    let mut ex: Vec<KeyExample> = Vec::new();
    for item in db.iter().take(examples) {
        let (k, v) = item?;
        let (bucket, utf8, looks_like_txid, _) = bucket_key(&k);
        ex.push(KeyExample {
            key_utf8: utf8,
            key_hex: hex::encode(&k),
            value_len: v.len(),
            bucket,
            looks_like_txid,
        });
    }

    Ok(DefaultTreeInspect {
        tree: "__sled__default".to_string(),
        len,
        sample,
        sampled,
        looks_like_txid_keys,
        non_utf8_keys,
        top_prefixes,
        examples: ex,
    })
}

// ---------------------------------------------------------------------------
// HTTP-facing function names (used by api.rs)
// ---------------------------------------------------------------------------

pub fn inspect_default(
    db: &sled::Db,
    sample: usize,
    top: usize,
    examples: usize,
) -> Result<DefaultTreeInspect> {
    inspect_default_tree(db, sample, top, examples)
}

pub fn prefix_stats(db: &sled::Db, sample: usize, top: usize) -> Result<PrefixStatsOut> {
    let len: u64 = db.len().try_into().unwrap();
    let (sampled, top_prefixes, looks_like_txid_keys, non_utf8_keys) =
        default_prefix_stats(db, sample, top)?;
    Ok(PrefixStatsOut {
        tree: "__sled__default".to_string(),
        len,
        sample,
        sampled,
        looks_like_txid_keys,
        non_utf8_keys,
        top_prefixes,
    })
}

pub fn preview_keys(db: &sled::Db, limit: usize) -> Result<PreviewKeysOut> {
    let len: u64 = db.len().try_into().unwrap();
    let mut keys: Vec<PreviewKeyRow> = Vec::new();
    for item in db.iter().take(limit) {
        let (k, v) = item?;
        let (bucket, utf8, looks_like_txid, _) = bucket_key(&k);
        keys.push(PreviewKeyRow {
            key_utf8: utf8,
            key_hex: hex::encode(&k),
            value_len: v.len(),
            bucket,
            looks_like_txid,
        });
    }
    Ok(PreviewKeysOut {
        tree: "__sled__default".to_string(),
        len,
        limit,
        keys,
    })
}

// ---------------------------------------------------------------------------
// CLI runner
// ---------------------------------------------------------------------------

pub fn run_cli(db: sled::Db, cmd: Cmd) -> Result<()> {
    match cmd {
        Cmd::Scan { tree, limit, prefix, show_values } => {
            let prefix_bytes = prefix.as_deref().map(parse_key_spec).transpose()?;
            let mut count = 0usize;

            let mut iter_fn =
                |iter: Box<dyn Iterator<Item = Result<(IVec, IVec), sled::Error>>>| {
                    for item in iter {
                        let (k, v) = item?;
                        count += 1;
                        print_entry(&k, &v, show_values);
                        if count >= limit { break; }
                    }
                    Ok::<_, anyhow::Error>(count)
                };

            if tree == "__sled__default" {
                let iter: Box<dyn Iterator<Item = _>> = match prefix_bytes {
                    Some(p) => Box::new(db.scan_prefix(p)),
                    None => Box::new(db.iter()),
                };
                count = iter_fn(iter)?;
            } else {
                let t = db.open_tree(&tree).with_context(|| format!("open tree {tree}"))?;
                let iter: Box<dyn Iterator<Item = _>> = match prefix_bytes {
                    Some(p) => Box::new(t.scan_prefix(p)),
                    None => Box::new(t.iter()),
                };
                count = iter_fn(iter)?;
            }
            println!("printed {count} entries");
        }

        Cmd::Get { tree, key, show_value } => {
            let k = parse_key_spec(&key)?;
            let v = if tree == "__sled__default" {
                db.get(&k)?.ok_or_else(|| anyhow!("key not found"))?
            } else {
                let t = db.open_tree(&tree).with_context(|| format!("open tree {tree}"))?;
                t.get(&k)?.ok_or_else(|| anyhow!("key not found"))?
            };
            println!("key_len={} value_len={}", k.len(), v.len());
            println!("key_hex={}", hex::encode(&k));
            if show_value { println!("value_hex={}", hex::encode(&v)); }
        }

        Cmd::DecodeUtxos { txid } => {
            let decoded = decode_utxo_entry(&db, &txid)?;
            print_decoded_outputs(&decoded.txid, &decoded.outputs);
        }

        Cmd::UtxosForAddress { address, limit } => {
            let addr = Public::new_from_base36(&address)
                .ok_or_else(|| anyhow!("invalid address base36"))?;
            let mut printed = 0usize;
            for item in db.iter() {
                let (k, v) = item?;
                let txid_str = match String::from_utf8(k.to_vec()) {
                    Ok(s) => s,
                    Err(_) => continue,
                };
                let Some(txid_obj) = TransactionId::new_from_base36(&txid_str) else { continue };
                let Ok((outputs, _)) =
                    bincode::decode_from_slice::<Vec<Option<TransactionOutput>>, _>(
                        &v,
                        bincode::config::standard(),
                    ) else { continue; };
                if outputs.iter().any(|o| o.is_some_and(|o| o.receiver == addr)) {
                    printed += 1;
                    println!("{}", txid_obj.dump_base36());
                    if printed >= limit { break; }
                }
            }
            println!("printed {printed} txids");
        }

        Cmd::Balance { address } => {
            let addr = Public::new_from_base36(&address)
                .ok_or_else(|| anyhow!("invalid address base36"))?;
            let bal = confirmed_balance(&db, &addr)?;
            println!("address={}", addr.dump_base36());
            println!("confirmed_balance={bal}");
        }

        Cmd::TopReceivers { limit } => {
            let top = top_receivers(&db, limit)?;
            for (i, row) in top.iter().enumerate() {
                println!(
                    "#{:02} receiver={} total_unspent={}",
                    i + 1, row.receiver_base36, row.total_unspent
                );
            }
        }

        Cmd::Globals => {
            let g = global_metrics(&db)?;
            println!("{}", serde_json::to_string_pretty(&g)?);
        }

        Cmd::WalletIntelligence => {
            let wi = wallet_intelligence(&db)?;
            println!("{}", serde_json::to_string_pretty(&wi)?);
        }

        Cmd::WalletStats { address } => {
            let addr = Public::new_from_base36(&address)
                .ok_or_else(|| anyhow!("invalid address base36"))?;
            let ws = wallet_stats(&db, &addr)?;
            println!("{}", serde_json::to_string_pretty(&ws)?);
        }

        Cmd::InspectDefault { sample, top, examples } => {
            let out = inspect_default_tree(&db, sample, top, examples)?;
            println!("{}", serde_json::to_string_pretty(&out)?);
        }

        Cmd::PrefixStats { sample, top } => {
            let (sampled, rows, txid_like, non_utf8) = default_prefix_stats(&db, sample, top)?;
            let out = serde_json::json!({
                "tree": "__sled__default",
                "len": db.len(),
                "sample": sample,
                "sampled": sampled,
                "looks_like_txid_keys": txid_like,
                "non_utf8_keys": non_utf8,
                "top_prefixes": rows
            });
            println!("{}", serde_json::to_string_pretty(&out)?);
        }

        Cmd::PreviewKeys { limit } => {
            let mut printed = 0usize;
            for item in db.iter().take(limit) {
                let (k, v) = item?;
                let (bucket, utf8, looks_like_txid, _) = bucket_key(&k);
                let k_hex = hex::encode(&k);
                if let Some(s) = utf8 {
                    println!(
                        "key=utf8:{}  key_hex:{}  value_len={}  bucket={}  txid={}",
                        s, k_hex, v.len(), bucket, looks_like_txid
                    );
                } else {
                    println!(
                        "key_hex:{}  value_len={}  bucket={}  txid={}",
                        k_hex, v.len(), bucket, looks_like_txid
                    );
                }
                printed += 1;
            }
            println!("printed {printed} keys");
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Helper: print sled entry
// ---------------------------------------------------------------------------

fn print_entry(k: &IVec, v: &IVec, show_values: bool) {
    let k_hex = hex::encode(k);
    match String::from_utf8(k.to_vec())
        .ok()
        .filter(|s| s.chars().all(|c| !c.is_control()))
    {
        Some(s) => println!("key=utf8:{s}  key_hex:{k_hex}  value_len={}", v.len()),
        None    => println!("key_hex:{k_hex}  value_len={}", v.len()),
    }
    if show_values { println!("  value_hex:{}", hex::encode(v)); }
}

// ---------------------------------------------------------------------------
// Helper: parse key spec
// ---------------------------------------------------------------------------

pub fn parse_key_spec(spec: &str) -> Result<Vec<u8>> {
    if let Some(rest) = spec.strip_prefix("hex:") {
        Ok(hex::decode(rest.trim()).with_context(|| format!("invalid hex key spec: {spec}"))?)
    } else if let Some(rest) = spec.strip_prefix("utf8:") {
        Ok(rest.as_bytes().to_vec())
    } else {
        Ok(spec.as_bytes().to_vec())
    }
}

// ---------------------------------------------------------------------------
// Decoded output types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize)]
pub struct DecodedUtxo {
    pub txid: String,
    pub outputs: Vec<DecodedOutput>,
}

#[derive(Debug, Clone, Serialize)]
pub struct DecodedOutput {
    pub index: usize,
    pub state: String,  // "spent" | "unspent"
    pub amount: Option<u64>,
    pub receiver: Option<String>,
}

// ---------------------------------------------------------------------------
// decode_utxo_entry
// ---------------------------------------------------------------------------

pub fn decode_utxo_entry(db: &sled::Db, txid_b36: &str) -> Result<DecodedUtxo> {
    let txid_obj = TransactionId::new_from_base36(txid_b36)
        .ok_or_else(|| anyhow!("invalid txid base36"))?;
    let key = txid_obj.dump_base36();
    let v = db
        .get(key.as_bytes())?
        .ok_or_else(|| anyhow!("txid not found in db"))?;

    let (outputs, _): (Vec<Option<TransactionOutput>>, usize) =
        bincode::decode_from_slice(&v, bincode::config::standard())
            .map_err(|e| anyhow!("bincode decode failed: {e}"))?;

    let decoded = outputs
        .iter()
        .enumerate()
        .map(|(i, o)| match o {
            Some(out) => DecodedOutput {
                index: i,
                state: "unspent".into(),
                amount: Some(out.amount),
                receiver: Some(out.receiver.dump_base36()),
            },
            None => DecodedOutput {
                index: i,
                state: "spent".into(),
                amount: None,
                receiver: None,
            },
        })
        .collect();

    Ok(DecodedUtxo {
        txid: txid_obj.dump_base36(),
        outputs: decoded,
    })
}

fn print_decoded_outputs(txid: &str, outputs: &[DecodedOutput]) {
    println!("txid={txid}");
    println!("outputs={}", outputs.len());
    for o in outputs {
        if o.state == "spent" {
            println!("#{}: spent", o.index);
        } else {
            println!(
                "#{}: amount={} receiver={}",
                o.index,
                o.amount.unwrap_or(0),
                o.receiver.as_deref().unwrap_or("")
            );
        }
    }
}

// ---------------------------------------------------------------------------
// confirmed_balance
// ---------------------------------------------------------------------------

pub fn confirmed_balance(db: &sled::Db, address: &Public) -> Result<u64> {
    let mut sum: u64 = 0;
    for item in db.iter() {
        let (_k, v) = item?;
        let Ok((outputs, _)) = bincode::decode_from_slice::<Vec<Option<TransactionOutput>>, _>(
            &v,
            bincode::config::standard(),
        ) else { continue; };
        for o in outputs.into_iter().flatten() {
            if &o.receiver == address {
                sum = sum.saturating_add(o.amount);
            }
        }
    }
    Ok(sum)
}

// ---------------------------------------------------------------------------
// top_receivers
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize)]
pub struct TopReceiverRow {
    pub receiver_base36: String,
    pub total_unspent: u64,
}

pub fn top_receivers(db: &sled::Db, limit: usize) -> Result<Vec<TopReceiverRow>> {
    let mut map: HashMap<String, u64> = HashMap::new();
    for item in db.iter() {
        let (_k, v) = item?;
        let Ok((outputs, _)) = bincode::decode_from_slice::<Vec<Option<TransactionOutput>>, _>(
            &v,
            bincode::config::standard(),
        ) else { continue; };
        for o in outputs.into_iter().flatten() {
            let entry = map.entry(o.receiver.dump_base36()).or_insert(0);
            *entry = entry.saturating_add(o.amount);
        }
    }
    let mut rows: Vec<TopReceiverRow> = map
        .into_iter()
        .map(|(receiver_base36, total_unspent)| TopReceiverRow { receiver_base36, total_unspent })
        .collect();
    rows.sort_by(|a, b| b.total_unspent.cmp(&a.total_unspent));
    rows.truncate(limit);
    Ok(rows)
}

// =============================================================================
// src/db.rs
// snapcoin-db-inspector/src/db.rs
// Created: 2026-02-26T00:00:00Z
// =============================================================================
