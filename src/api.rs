// =============================================================================
// src/api.rs
// snapcoin-db-inspector/src/api.rs
// v0.4.0-wallet-intelligence.1
// Axum HTTP route handlers. Serves static dashboard and JSON API endpoints.
//
// Change (v0.4.0 ... .1):
// - ADD GET /api/wallet_intelligence
//       Returns global derived intelligence metrics from a single full DB scan:
//       most active wallet, most fragmented, fastest growing, highest spend rate,
//       largest single output. Used to populate the global intelligence panel.
// - ADD GET /api/wallet_stats/:address
//       Returns per-address summary: tx count, unspent/spent counts, balance,
//       baseline balance, delta, trend, fragmentation rating, first/last seen txid.
//       Used to populate the per-wallet summary strip on inspect.
//
// Existing endpoints unchanged:
//   GET /api/globals
//   GET /api/get/:txid
//   GET /api/decode/:txid
//   GET /api/utxos_for/:address
//   GET /api/balance/:address
//   GET /api/top_receivers
//   GET /api/inspect_default
//   GET /api/prefix_stats
//   GET /api/preview_keys
// =============================================================================

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{Html, IntoResponse, Response},
    Json,
};
use serde::{Deserialize, Serialize};
use tokio::task;

use crate::db::{
    confirmed_balance,
    decode_utxo_entry,
    global_metrics,
    top_receivers,
    wallet_intelligence,
    wallet_stats,
    AppState,
    GlobalMetrics,
    WalletIntelligence,
    WalletStats,
};

use snap_coin::core::transaction::TransactionId;
use snap_coin::crypto::keys::Public;

// ---------------------------------------------------------------------------
// Dashboard HTML (served from static/)
// ---------------------------------------------------------------------------

static INDEX_HTML: &str = include_str!("../static/db_dashboard.html");

pub async fn ui_index() -> Html<&'static str> {
    Html(INDEX_HTML)
}

// ---------------------------------------------------------------------------
// GET /api/globals
// ---------------------------------------------------------------------------

pub async fn api_globals(State(st): State<AppState>) -> Response {
    match task::spawn_blocking({
        let db = st.db.clone();
        move || -> anyhow::Result<GlobalMetrics> { global_metrics(&db) }
    })
    .await
    {
        Ok(Ok(g))  => Json(g).into_response(),
        Ok(Err(e)) => (StatusCode::BAD_REQUEST, e.to_string()).into_response(),
        Err(e)     => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

// ---------------------------------------------------------------------------
// GET /api/get/:txid  →  raw hex value
// ---------------------------------------------------------------------------

#[derive(Serialize)]
pub struct ApiGetResp {
    pub txid: String,
    pub value_hex: String,
}

pub async fn api_get_txid(
    State(st): State<AppState>,
    Path(txid): Path<String>,
) -> Response {
    let txid_work = txid.clone();
    match task::spawn_blocking({
        let db = st.db.clone();
        move || -> anyhow::Result<Option<Vec<u8>>> {
            let txid_obj = TransactionId::new_from_base36(&txid_work)
                .ok_or_else(|| anyhow::anyhow!("invalid txid base36"))?;
            Ok(db.get(txid_obj.dump_base36().as_bytes())?.map(|v| v.to_vec()))
        }
    })
    .await
    {
        Ok(Ok(Some(v))) => Json(ApiGetResp { txid, value_hex: hex::encode(v) }).into_response(),
        Ok(Ok(None))    => (StatusCode::NOT_FOUND, "not found").into_response(),
        Ok(Err(e))      => (StatusCode::BAD_REQUEST, e.to_string()).into_response(),
        Err(e)          => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

// ---------------------------------------------------------------------------
// GET /api/decode/:txid
// ---------------------------------------------------------------------------

pub async fn api_decode_txid(
    State(st): State<AppState>,
    Path(txid): Path<String>,
) -> Response {
    let txid_work = txid.clone();
    match task::spawn_blocking({
        let db = st.db.clone();
        move || decode_utxo_entry(&db, &txid_work)
    })
    .await
    {
        Ok(Ok(d))  => Json(d).into_response(),
        Ok(Err(e)) => (StatusCode::BAD_REQUEST, e.to_string()).into_response(),
        Err(e)     => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

// ---------------------------------------------------------------------------
// GET /api/utxos_for/:address
// ---------------------------------------------------------------------------

pub async fn api_utxos_for(
    State(st): State<AppState>,
    Path(address): Path<String>,
) -> Response {
    let addr_work = address.clone();
    match task::spawn_blocking({
        let db = st.db.clone();
        move || -> anyhow::Result<Vec<String>> {
            let addr = Public::new_from_base36(&addr_work)
                .ok_or_else(|| anyhow::anyhow!("invalid address base36"))?;
            let mut out = Vec::new();
            for item in db.iter() {
                let (k, v) = item?;
                let txid_str = match String::from_utf8(k.to_vec()) {
                    Ok(s) => s,
                    Err(_) => continue,
                };
                let Some(_) = snap_coin::core::transaction::TransactionId::new_from_base36(
                    &txid_str,
                ) else {
                    continue;
                };
                let Ok((outputs, _)) = bincode::decode_from_slice::<
                    Vec<Option<snap_coin::core::transaction::TransactionOutput>>,
                    _,
                >(&v, bincode::config::standard()) else {
                    continue;
                };
                if outputs.iter().any(|o| o.is_some_and(|o| o.receiver == addr)) {
                    out.push(txid_str);
                }
            }
            Ok(out)
        }
    })
    .await
    {
        Ok(Ok(out)) => Json(out).into_response(),
        Ok(Err(e))  => (StatusCode::BAD_REQUEST, e.to_string()).into_response(),
        Err(e)      => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

// ---------------------------------------------------------------------------
// GET /api/balance/:address
// ---------------------------------------------------------------------------

#[derive(Serialize)]
pub struct ApiBalanceResp {
    pub address: String,
    pub confirmed_balance: u64,
}

pub async fn api_balance(
    State(st): State<AppState>,
    Path(address): Path<String>,
) -> Response {
    let addr_work = address.clone();
    match task::spawn_blocking({
        let db = st.db.clone();
        move || -> anyhow::Result<u64> {
            let addr = Public::new_from_base36(&addr_work)
                .ok_or_else(|| anyhow::anyhow!("invalid address base36"))?;
            confirmed_balance(&db, &addr)
        }
    })
    .await
    {
        Ok(Ok(bal)) => Json(ApiBalanceResp {
            address,
            confirmed_balance: bal,
        })
        .into_response(),
        Ok(Err(e)) => (StatusCode::BAD_REQUEST, e.to_string()).into_response(),
        Err(e)     => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

// ---------------------------------------------------------------------------
// GET /api/top_receivers
// ---------------------------------------------------------------------------

pub async fn api_top_receivers(State(st): State<AppState>) -> Response {
    match task::spawn_blocking({
        let db = st.db.clone();
        move || top_receivers(&db, 9999)
    })
    .await
    {
        Ok(Ok(rows)) => Json(rows).into_response(),
        Ok(Err(e))   => (StatusCode::BAD_REQUEST, e.to_string()).into_response(),
        Err(e)       => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

// ---------------------------------------------------------------------------
// GET /api/wallet_intelligence
//
// Single full DB scan returning global derived intelligence metrics:
//   - most_active_wallet         (highest tx count)
//   - most_fragmented_wallet     (highest unspent output count)
//   - fastest_growing_wallet     (largest delta: current - baseline unspent)
//   - highest_spend_rate_wallet  (highest spent/total ratio, min 5 outputs)
//   - largest_single_output      (amount + txid + receiver)
//
// This is a heavy scan — same cost as globals. Cache result on the client
// and refresh only on explicit user request or page load.
// ---------------------------------------------------------------------------

pub async fn api_wallet_intelligence(State(st): State<AppState>) -> Response {
    match task::spawn_blocking({
        let db = st.db.clone();
        move || -> anyhow::Result<WalletIntelligence> { wallet_intelligence(&db) }
    })
    .await
    {
        Ok(Ok(wi)) => Json(wi).into_response(),
        Ok(Err(e)) => (StatusCode::BAD_REQUEST, e.to_string()).into_response(),
        Err(e)     => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

// ---------------------------------------------------------------------------
// GET /api/wallet_stats/:address
//
// Per-address summary derived from a single filtered scan:
//   tx_count, unspent_count, spent_count, total_unspent_atomic,
//   baseline_unspent_atomic (first-seen snapshot), delta, trend,
//   largest_output, avg_output, fragmentation rating (LOW/MEDIUM/HIGH),
//   first_seen_txid, last_seen_txid, first_seen_index, last_seen_index.
//
// Called automatically when a wallet is inspected to populate the
// per-wallet summary strip above the UTXO timeline.
// ---------------------------------------------------------------------------

pub async fn api_wallet_stats(
    State(st): State<AppState>,
    Path(address): Path<String>,
) -> Response {
    let addr_work = address.clone();
    match task::spawn_blocking({
        let db = st.db.clone();
        move || -> anyhow::Result<WalletStats> {
            let addr = Public::new_from_base36(&addr_work)
                .ok_or_else(|| anyhow::anyhow!("invalid address base36"))?;
            wallet_stats(&db, &addr)
        }
    })
    .await
    {
        Ok(Ok(ws)) => Json(ws).into_response(),
        Ok(Err(e)) => (StatusCode::BAD_REQUEST, e.to_string()).into_response(),
        Err(e)     => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

// ---------------------------------------------------------------------------
// Default sled tree inspector endpoints (unchanged)
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
pub struct InspectDefaultQ {
    pub sample: Option<usize>,
    pub top: Option<usize>,
    pub examples: Option<usize>,
}

pub async fn api_inspect_default(
    State(st): State<AppState>,
    Query(q): Query<InspectDefaultQ>,
) -> Response {
    let sample   = q.sample.unwrap_or(10_000);
    let top      = q.top.unwrap_or(30);
    let examples = q.examples.unwrap_or(30);

    match task::spawn_blocking({
        let db = st.db.clone();
        move || crate::db::inspect_default(&db, sample, top, examples)
    })
    .await
    {
        Ok(Ok(out)) => Json(out).into_response(),
        Ok(Err(e))  => (StatusCode::BAD_REQUEST, e.to_string()).into_response(),
        Err(e)      => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

#[derive(Deserialize)]
pub struct PrefixStatsQ {
    pub sample: Option<usize>,
    pub top: Option<usize>,
}

pub async fn api_prefix_stats(
    State(st): State<AppState>,
    Query(q): Query<PrefixStatsQ>,
) -> Response {
    let sample = q.sample.unwrap_or(20_000);
    let top    = q.top.unwrap_or(40);

    match task::spawn_blocking({
        let db = st.db.clone();
        move || crate::db::prefix_stats(&db, sample, top)
    })
    .await
    {
        Ok(Ok(out)) => Json(out).into_response(),
        Ok(Err(e))  => (StatusCode::BAD_REQUEST, e.to_string()).into_response(),
        Err(e)      => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

#[derive(Deserialize)]
pub struct PreviewKeysQ {
    pub limit: Option<usize>,
}

pub async fn api_preview_keys(
    State(st): State<AppState>,
    Query(q): Query<PreviewKeysQ>,
) -> Response {
    let limit = q.limit.unwrap_or(100);

    match task::spawn_blocking({
        let db = st.db.clone();
        move || crate::db::preview_keys(&db, limit)
    })
    .await
    {
        Ok(Ok(out)) => Json(out).into_response(),
        Ok(Err(e))  => (StatusCode::BAD_REQUEST, e.to_string()).into_response(),
        Err(e)      => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

// =============================================================================
// src/api.rs
// snapcoin-db-inspector/src/api.rs
// Created: 2026-02-26T00:00:00Z
// =============================================================================
