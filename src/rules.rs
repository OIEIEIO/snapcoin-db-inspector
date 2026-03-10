// =============================================================================
// File: src/rules.rs
// Path: snapcoin-db-inspector/src/rules.rs
// Version: v0.2.0-db-matched-tx-anomaly-rules.1
// Git Branch: dev
//
// Sentinel anomaly rules matched to the current DB model.
//
// Current DB model:
// - default sled tree
// - key   = txid_base36
// - value = bincode(Vec<Option<TransactionOutput>>)
//
// This file contains pure rule evaluation logic for tx-entry anomalies.
//
// Implemented rules:
// - RULE-OUTPUT-001 : abnormal single output value
// - RULE-TXTOTAL-001: abnormal tx total unspent value
// - RULE-RECV-001   : receiver concentration spike within one tx
// =============================================================================

use crate::events::{
    EVENT_TYPE_ABNORMAL_OUTPUT, EVENT_TYPE_SUPPLY_JUMP, SEVERITY_CRITICAL, SEVERITY_HIGH,
    SEVERITY_MEDIUM, RULE_OUTPUT_001, SentinelAmountSummary, SentinelAnomaly,
    SentinelBlockContext, SentinelDeltaSummary, SentinelDisposition, SentinelEvent,
    SentinelEvidence, SentinelImpact, SentinelLinks, SentinelNetworkContext,
    SentinelObservedSummary, STATUS_OPEN,
};

/// =============================================================================
/// Local rule constants
/// =============================================================================

pub const RULE_TXTOTAL_001: &str = "RULE-TXTOTAL-001";
pub const RULE_RECV_001: &str = "RULE-RECV-001";

/// =============================================================================
/// Normalized tx facts
/// =============================================================================

#[derive(Debug, Clone)]
pub struct TxOutputFact {
    pub index: u32,
    pub address: String,
    pub amount_atomic: u64,
    pub amount_display: String,
}

#[derive(Debug, Clone)]
pub struct ReceiverAggregateFact {
    pub address: String,
    pub output_count: usize,
    pub total_atomic: u64,
    pub total_display: String,
}

#[derive(Debug, Clone)]
pub struct TxRuleInput {
    pub detected_at: String,

    pub network: SentinelNetworkContext,

    /// We do not have real block metadata from the current DB model,
    /// so we populate a tx-anchored pseudo-block context.
    pub block: SentinelBlockContext,

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

#[derive(Debug, Clone)]
pub struct TxRuleThresholds {
    pub abnormal_output_atomic: u64,
    pub abnormal_tx_total_atomic: u64,
    pub receiver_concentration_atomic: u64,
}

impl Default for TxRuleThresholds {
    fn default() -> Self {
        Self {
            abnormal_output_atomic: 1_000_000u64 * 100_000_000u64,
            abnormal_tx_total_atomic: 5_000_000u64 * 100_000_000u64,
            receiver_concentration_atomic: 1_000_000u64 * 100_000_000u64,
        }
    }
}

/// =============================================================================
/// Rule outputs
/// =============================================================================

#[derive(Debug, Clone)]
pub struct RuleMatch {
    pub event: SentinelEvent,
}

#[derive(Debug, Clone, Default)]
pub struct RuleSetResult {
    pub matches: Vec<RuleMatch>,
}

impl RuleSetResult {
    pub fn push(&mut self, rule_match: RuleMatch) {
        self.matches.push(rule_match);
    }

    pub fn is_empty(&self) -> bool {
        self.matches.is_empty()
    }
}

/// =============================================================================
/// RULE-OUTPUT-001
/// Abnormal single output value.
/// =============================================================================

pub fn evaluate_abnormal_output_value(
    input: &TxRuleInput,
    thresholds: &TxRuleThresholds,
) -> Vec<RuleMatch> {
    let mut out = Vec::new();

    for output in &input.outputs {
        if output.amount_atomic < thresholds.abnormal_output_atomic {
            continue;
        }

        let event_id = format!(
            "sentinel-{}-out-{}-{}",
            input.txid,
            output.index,
            RULE_OUTPUT_001.to_lowercase()
        );

        let anomaly = SentinelAnomaly {
            rule_id: RULE_OUTPUT_001.to_string(),
            rule_name: "single output exceeds abnormal output threshold".to_string(),
            category: "tx-anomaly".to_string(),
            confidence: 0.98,
            expected: SentinelAmountSummary {
                subsidy_atomic: None,
                fees_atomic: None,
                allowed_coinbase_atomic: None,
                extra: Default::default(),
            },
            observed: SentinelObservedSummary {
                coinbase_total_atomic: None,
                coinbase_output_count: Some(input.output_count),
                extra: {
                    let mut m = serde_json::Map::new();
                    m.insert("txid".to_string(), serde_json::json!(input.txid));
                    m.insert("output_index".to_string(), serde_json::json!(output.index));
                    m.insert("output_amount_atomic".to_string(), serde_json::json!(output.amount_atomic));
                    m.insert("output_address".to_string(), serde_json::json!(output.address));
                    m
                },
            },
            delta: SentinelDeltaSummary {
                excess_atomic: Some((output.amount_atomic - thresholds.abnormal_output_atomic) as u128),
                reward_ratio: Some(output.amount_atomic as f64 / thresholds.abnormal_output_atomic as f64),
                extra: Default::default(),
            },
        };

        let evidence = SentinelEvidence {
            coinbase_outputs: vec![crate::events::SentinelOutputEvidence {
                index: output.index,
                address: output.address.clone(),
                amount_atomic: output.amount_atomic as u128,
                amount_display: output.amount_display.clone(),
            }],
            flags: vec![
                "abnormal_output_value".to_string(),
                "tx_anomaly".to_string(),
                "review_required".to_string(),
            ],
            extra: Default::default(),
        };

        let impact = SentinelImpact {
            estimated_supply_increase_atomic: None,
            estimated_supply_increase_display: None,
            potential_exchange_risk: Some(true),
            potential_consensus_risk: Some(false),
            extra: Default::default(),
        };

        let disposition = SentinelDisposition {
            should_page: Some(true),
            should_freeze_exchange: Some(false),
            should_freeze_pool: Some(false),
            requires_manual_review: Some(true),
        };

        let links = SentinelLinks {
            explorer_block_url: None,
            explorer_tx_url: Some(format!("/api/decode/{}", input.txid)),
            internal_api_url: Some(format!("/api/get/{}", input.txid)),
        };

        let event = SentinelEvent::new(
            event_id,
            EVENT_TYPE_ABNORMAL_OUTPUT,
            SEVERITY_HIGH,
            STATUS_OPEN,
            input.detected_at.clone(),
            input.network.clone(),
            input.block.clone(),
            anomaly,
            evidence,
        )
        .with_impact(impact)
        .with_disposition(disposition)
        .with_links(links)
        .with_actions(vec![
            "inspect_tx_outputs".to_string(),
            "trace_receiver_address".to_string(),
            "review_exchange_flow".to_string(),
        ])
        .with_notes(vec![
            "Detected by RULE-OUTPUT-001".to_string(),
            "Single tx output exceeded configured threshold".to_string(),
        ]);

        out.push(RuleMatch { event });
    }

    out
}

/// =============================================================================
/// RULE-TXTOTAL-001
/// Abnormal total unspent value in a single tx entry.
/// =============================================================================

pub fn evaluate_abnormal_tx_total(
    input: &TxRuleInput,
    thresholds: &TxRuleThresholds,
) -> Option<RuleMatch> {
    if input.tx_total_unspent_atomic < thresholds.abnormal_tx_total_atomic {
        return None;
    }

    let event_id = format!(
        "sentinel-{}-{}",
        input.txid,
        RULE_TXTOTAL_001.to_lowercase()
    );

    let anomaly = SentinelAnomaly {
        rule_id: RULE_TXTOTAL_001.to_string(),
        rule_name: "tx total unspent value exceeds abnormal tx threshold".to_string(),
        category: "tx-anomaly".to_string(),
        confidence: 0.99,
        expected: SentinelAmountSummary {
            subsidy_atomic: None,
            fees_atomic: None,
            allowed_coinbase_atomic: None,
            extra: {
                let mut m = serde_json::Map::new();
                m.insert(
                    "abnormal_tx_total_threshold_atomic".to_string(),
                    serde_json::json!(thresholds.abnormal_tx_total_atomic),
                );
                m
            },
        },
        observed: SentinelObservedSummary {
            coinbase_total_atomic: None,
            coinbase_output_count: Some(input.output_count),
            extra: {
                let mut m = serde_json::Map::new();
                m.insert("txid".to_string(), serde_json::json!(input.txid));
                m.insert(
                    "tx_total_unspent_atomic".to_string(),
                    serde_json::json!(input.tx_total_unspent_atomic),
                );
                m.insert("unspent_count".to_string(), serde_json::json!(input.unspent_count));
                m.insert("spent_count".to_string(), serde_json::json!(input.spent_count));
                m
            },
        },
        delta: SentinelDeltaSummary {
            excess_atomic: Some(
                (input.tx_total_unspent_atomic - thresholds.abnormal_tx_total_atomic) as u128
            ),
            reward_ratio: Some(
                input.tx_total_unspent_atomic as f64 / thresholds.abnormal_tx_total_atomic as f64
            ),
            extra: Default::default(),
        },
    };

    let evidence = SentinelEvidence {
        coinbase_outputs: input
            .outputs
            .iter()
            .map(|o| crate::events::SentinelOutputEvidence {
                index: o.index,
                address: o.address.clone(),
                amount_atomic: o.amount_atomic as u128,
                amount_display: o.amount_display.clone(),
            })
            .collect(),
        flags: vec![
            "abnormal_tx_total".to_string(),
            "large_value_cluster".to_string(),
            "review_required".to_string(),
        ],
        extra: Default::default(),
    };

    let impact = SentinelImpact {
        estimated_supply_increase_atomic: None,
        estimated_supply_increase_display: None,
        potential_exchange_risk: Some(true),
        potential_consensus_risk: Some(true),
        extra: Default::default(),
    };

    let disposition = SentinelDisposition {
        should_page: Some(true),
        should_freeze_exchange: Some(false),
        should_freeze_pool: Some(false),
        requires_manual_review: Some(true),
    };

    let links = SentinelLinks {
        explorer_block_url: None,
        explorer_tx_url: Some(format!("/api/decode/{}", input.txid)),
        internal_api_url: Some(format!("/api/get/{}", input.txid)),
    };

    let event = SentinelEvent::new(
        event_id,
        EVENT_TYPE_SUPPLY_JUMP,
        SEVERITY_CRITICAL,
        STATUS_OPEN,
        input.detected_at.clone(),
        input.network.clone(),
        input.block.clone(),
        anomaly,
        evidence,
    )
    .with_impact(impact)
    .with_disposition(disposition)
    .with_links(links)
    .with_actions(vec![
        "inspect_tx_outputs".to_string(),
        "compare_against_expected_supply".to_string(),
        "trace_funds".to_string(),
        "review_consensus_rules".to_string(),
    ])
    .with_notes(vec![
        "Detected by RULE-TXTOTAL-001".to_string(),
        "Single tx entry exceeded configured total value threshold".to_string(),
    ]);

    Some(RuleMatch { event })
}

/// =============================================================================
/// RULE-RECV-001
/// Receiver concentration spike in a single tx.
/// =============================================================================

pub fn evaluate_receiver_concentration(
    input: &TxRuleInput,
    thresholds: &TxRuleThresholds,
) -> Vec<RuleMatch> {
    let mut out = Vec::new();

    for agg in &input.receiver_aggregates {
        if agg.total_atomic < thresholds.receiver_concentration_atomic {
            continue;
        }

        let event_id = format!(
            "sentinel-{}-recv-{}-{}",
            input.txid,
            agg.address,
            RULE_RECV_001.to_lowercase()
        );

        let anomaly = SentinelAnomaly {
            rule_id: RULE_RECV_001.to_string(),
            rule_name: "receiver concentration spike detected within one tx".to_string(),
            category: "receiver-anomaly".to_string(),
            confidence: 0.96,
            expected: SentinelAmountSummary {
                subsidy_atomic: None,
                fees_atomic: None,
                allowed_coinbase_atomic: None,
                extra: {
                    let mut m = serde_json::Map::new();
                    m.insert(
                        "receiver_concentration_threshold_atomic".to_string(),
                        serde_json::json!(thresholds.receiver_concentration_atomic),
                    );
                    m
                },
            },
            observed: SentinelObservedSummary {
                coinbase_total_atomic: None,
                coinbase_output_count: Some(agg.output_count),
                extra: {
                    let mut m = serde_json::Map::new();
                    m.insert("txid".to_string(), serde_json::json!(input.txid));
                    m.insert("receiver".to_string(), serde_json::json!(agg.address));
                    m.insert("receiver_total_atomic".to_string(), serde_json::json!(agg.total_atomic));
                    m.insert("receiver_output_count".to_string(), serde_json::json!(agg.output_count));
                    m
                },
            },
            delta: SentinelDeltaSummary {
                excess_atomic: Some((agg.total_atomic - thresholds.receiver_concentration_atomic) as u128),
                reward_ratio: Some(agg.total_atomic as f64 / thresholds.receiver_concentration_atomic as f64),
                extra: Default::default(),
            },
        };

        let evidence = SentinelEvidence {
            coinbase_outputs: input
                .outputs
                .iter()
                .filter(|o| o.address == agg.address)
                .map(|o| crate::events::SentinelOutputEvidence {
                    index: o.index,
                    address: o.address.clone(),
                    amount_atomic: o.amount_atomic as u128,
                    amount_display: o.amount_display.clone(),
                })
                .collect(),
            flags: vec![
                "receiver_concentration_spike".to_string(),
                "large_receiver_cluster".to_string(),
                "review_required".to_string(),
            ],
            extra: Default::default(),
        };

        let impact = SentinelImpact {
            estimated_supply_increase_atomic: None,
            estimated_supply_increase_display: None,
            potential_exchange_risk: Some(true),
            potential_consensus_risk: Some(false),
            extra: Default::default(),
        };

        let disposition = SentinelDisposition {
            should_page: Some(false),
            should_freeze_exchange: Some(false),
            should_freeze_pool: Some(false),
            requires_manual_review: Some(true),
        };

        let links = SentinelLinks {
            explorer_block_url: None,
            explorer_tx_url: Some(format!("/api/decode/{}", input.txid)),
            internal_api_url: Some(format!("/api/get/{}", input.txid)),
        };

        let event = SentinelEvent::new(
            event_id,
            "receiver_concentration_spike",
            SEVERITY_MEDIUM,
            STATUS_OPEN,
            input.detected_at.clone(),
            input.network.clone(),
            input.block.clone(),
            anomaly,
            evidence,
        )
        .with_impact(impact)
        .with_disposition(disposition)
        .with_links(links)
        .with_actions(vec![
            "inspect_receiver_cluster".to_string(),
            "trace_address_activity".to_string(),
        ])
        .with_notes(vec![
            "Detected by RULE-RECV-001".to_string(),
            "One receiver accumulated an unusually large share of a tx entry".to_string(),
        ]);

        out.push(RuleMatch { event });
    }

    out
}

/// =============================================================================
/// Rule set entrypoint
/// =============================================================================

pub fn evaluate_all_tx_rules(
    input: &TxRuleInput,
    thresholds: &TxRuleThresholds,
) -> RuleSetResult {
    let mut out = RuleSetResult::default();

    for m in evaluate_abnormal_output_value(input, thresholds) {
        out.push(m);
    }

    if let Some(m) = evaluate_abnormal_tx_total(input, thresholds) {
        out.push(m);
    }

    for m in evaluate_receiver_concentration(input, thresholds) {
        out.push(m);
    }

    out
}

/// =============================================================================
/// Helpers
/// =============================================================================

pub fn format_atomic_8(v: u64) -> String {
    let whole = v / 100_000_000u64;
    let frac = v % 100_000_000u64;
    format!("{whole}.{frac:08}")
}

// =============================================================================
// File: src/rules.rs
// Path: snapcoin-db-inspector/src/rules.rs
// =============================================================================