// =============================================================================
// File: src/events.rs
// Path: snapcoin-db-inspector/src/events.rs
// Version: v0.1.1-sentinel-events-footer-fix.1
// Git Branch: dev
//
// Sentinel event schema types for anomaly detection / alert emission.
//
// This file defines the JSON payload structures for chain anomaly events,
// including:
// - event envelope
// - network context
// - block context
// - anomaly summary
// - evidence details
// - impact summary
// - recommended actions
// - links / notes / disposition
// =============================================================================

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// =============================================================================
/// Sentinel event envelope
/// =============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SentinelEvent {
    pub schema_version: String,
    pub event_id: String,
    pub event_type: String,
    pub severity: String,
    pub status: String,
    pub detected_at: String,

    pub network: SentinelNetworkContext,
    pub block: SentinelBlockContext,
    pub anomaly: SentinelAnomaly,

    pub evidence: SentinelEvidence,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub impact: Option<SentinelImpact>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub disposition: Option<SentinelDisposition>,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub recommended_actions: Vec<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub links: Option<SentinelLinks>,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub notes: Vec<String>,
}

impl SentinelEvent {
    /// Create a minimal sentinel event with empty/default optional sections.
    pub fn new(
        event_id: impl Into<String>,
        event_type: impl Into<String>,
        severity: impl Into<String>,
        status: impl Into<String>,
        detected_at: impl Into<String>,
        network: SentinelNetworkContext,
        block: SentinelBlockContext,
        anomaly: SentinelAnomaly,
        evidence: SentinelEvidence,
    ) -> Self {
        Self {
            schema_version: "1.0.0".to_string(),
            event_id: event_id.into(),
            event_type: event_type.into(),
            severity: severity.into(),
            status: status.into(),
            detected_at: detected_at.into(),
            network,
            block,
            anomaly,
            evidence,
            impact: None,
            disposition: None,
            recommended_actions: Vec::new(),
            links: None,
            notes: Vec::new(),
        }
    }

    /// Convenience helper for setting impact.
    pub fn with_impact(mut self, impact: SentinelImpact) -> Self {
        self.impact = Some(impact);
        self
    }

    /// Convenience helper for setting disposition.
    pub fn with_disposition(mut self, disposition: SentinelDisposition) -> Self {
        self.disposition = Some(disposition);
        self
    }

    /// Convenience helper for setting links.
    pub fn with_links(mut self, links: SentinelLinks) -> Self {
        self.links = Some(links);
        self
    }

    /// Convenience helper for adding recommended actions.
    pub fn with_actions(mut self, actions: Vec<String>) -> Self {
        self.recommended_actions = actions;
        self
    }

    /// Convenience helper for adding notes.
    pub fn with_notes(mut self, notes: Vec<String>) -> Self {
        self.notes = notes;
        self
    }
}

/// =============================================================================
/// Network / block context
/// =============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SentinelNetworkContext {
    pub chain: String,
    pub network_type: String,
    pub node_id: String,
    pub db_height: u64,
    pub best_tip_hash: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SentinelBlockContext {
    pub height: u64,
    pub hash: String,
    pub prev_hash: String,
    pub timestamp: String,
    pub tx_count: usize,
    pub coinbase_txid: String,
}

/// =============================================================================
/// Anomaly summary
/// =============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SentinelAnomaly {
    pub rule_id: String,
    pub rule_name: String,
    pub category: String,
    pub confidence: f64,
    pub expected: SentinelAmountSummary,
    pub observed: SentinelObservedSummary,
    pub delta: SentinelDeltaSummary,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SentinelAmountSummary {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub subsidy_atomic: Option<u128>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub fees_atomic: Option<u128>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub allowed_coinbase_atomic: Option<u128>,

    #[serde(default, skip_serializing_if = "serde_json::Map::is_empty")]
    pub extra: serde_json::Map<String, Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SentinelObservedSummary {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub coinbase_total_atomic: Option<u128>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub coinbase_output_count: Option<usize>,

    #[serde(default, skip_serializing_if = "serde_json::Map::is_empty")]
    pub extra: serde_json::Map<String, Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SentinelDeltaSummary {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub excess_atomic: Option<u128>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub reward_ratio: Option<f64>,

    #[serde(default, skip_serializing_if = "serde_json::Map::is_empty")]
    pub extra: serde_json::Map<String, Value>,
}

/// =============================================================================
/// Evidence
/// =============================================================================

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SentinelEvidence {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub coinbase_outputs: Vec<SentinelOutputEvidence>,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub flags: Vec<String>,

    #[serde(default, skip_serializing_if = "serde_json::Map::is_empty")]
    pub extra: serde_json::Map<String, Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SentinelOutputEvidence {
    pub index: u32,
    pub address: String,
    pub amount_atomic: u128,
    pub amount_display: String,
}

/// =============================================================================
/// Impact / disposition / links
/// =============================================================================

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SentinelImpact {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub estimated_supply_increase_atomic: Option<u128>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub estimated_supply_increase_display: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub potential_exchange_risk: Option<bool>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub potential_consensus_risk: Option<bool>,

    #[serde(default, skip_serializing_if = "serde_json::Map::is_empty")]
    pub extra: serde_json::Map<String, Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SentinelDisposition {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub should_page: Option<bool>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub should_freeze_exchange: Option<bool>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub should_freeze_pool: Option<bool>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub requires_manual_review: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SentinelLinks {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub explorer_block_url: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub explorer_tx_url: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub internal_api_url: Option<String>,
}

/// =============================================================================
/// Compact notification payload
/// =============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SentinelNotification {
    pub title: String,
    pub summary: String,
    pub severity: String,
    pub height: u64,
    pub event_type: String,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub recommended_actions: Vec<String>,
}

/// =============================================================================
/// Rule / severity / event constants
/// =============================================================================

pub const EVENT_TYPE_COINBASE_OVERPAY: &str = "coinbase_overpay";
pub const EVENT_TYPE_SUPPLY_JUMP: &str = "supply_jump_detected";
pub const EVENT_TYPE_ABNORMAL_OUTPUT: &str = "abnormal_output_value";
pub const EVENT_TYPE_DOUBLE_SPEND_SUSPECTED: &str = "double_spend_suspected";
pub const EVENT_TYPE_CONSENSUS_MISMATCH: &str = "consensus_mismatch_candidate";
pub const EVENT_TYPE_EXCHANGE_DUMP_SUSPECTED: &str = "exchange_dump_suspected";
pub const EVENT_TYPE_SERIALIZATION_ANOMALY: &str = "serialization_anomaly";
pub const EVENT_TYPE_FEE_OVERFLOW_SUSPECTED: &str = "fee_overflow_suspected";
pub const EVENT_TYPE_AMOUNT_OVERFLOW_SUSPECTED: &str = "amount_overflow_suspected";

pub const SEVERITY_INFO: &str = "info";
pub const SEVERITY_LOW: &str = "low";
pub const SEVERITY_MEDIUM: &str = "medium";
pub const SEVERITY_HIGH: &str = "high";
pub const SEVERITY_CRITICAL: &str = "critical";

pub const STATUS_OPEN: &str = "open";
pub const STATUS_ACKNOWLEDGED: &str = "acknowledged";
pub const STATUS_RESOLVED: &str = "resolved";

pub const RULE_COINBASE_001: &str = "RULE-COINBASE-001";
pub const RULE_SUPPLY_001: &str = "RULE-SUPPLY-001";
pub const RULE_OUTPUT_001: &str = "RULE-OUTPUT-001";
pub const RULE_FEE_001: &str = "RULE-FEE-001";
pub const RULE_UTXO_001: &str = "RULE-UTXO-001";
pub const RULE_SERIALIZE_001: &str = "RULE-SERIALIZE-001";
pub const RULE_FLOW_001: &str = "RULE-FLOW-001";

// =============================================================================
// File: src/events.rs
// Path: snapcoin-db-inspector/src/events.rs
// =============================================================================