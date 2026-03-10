// =============================================================================
// src/ws.rs
// snapcoin-db-inspector/src/ws.rs
// v0.2.2-globals.1
// WebSocket upgrade handler, command dispatch, WsCmd / WsResp types.
//
// Notes:
// - No changes required for the new default-tree HTTP endpoints.
// - WS remains for existing dashboard flows.
// =============================================================================

use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        State,
    },
    response::Response,
};
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use tokio::task;

use crate::api::ApiBalanceResp;
use crate::db::{
    confirmed_balance, decode_utxo_entry, global_metrics, top_receivers, AppState, GlobalMetrics,
};
use snap_coin::core::transaction::{TransactionId, TransactionOutput};
use snap_coin::crypto::keys::Public;

// ---------------------------------------------------------------------------
// WS command types
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
#[serde(tag = "cmd", rename_all = "snake_case")]
pub enum WsCmd {
    
    Globals,
    Decode { txid: String },
    Balance { address: String },
    UtxosFor { address: String, #[serde(default)] limit: Option<usize> },
    TopReceivers { #[serde(default)] limit: Option<usize> },
}

#[derive(Debug, Serialize)]
pub struct WsResp<T: Serialize> {
    pub ok: bool,
    pub cmd: String,
    pub data: Option<T>,
    pub error: Option<String>,
}

// ---------------------------------------------------------------------------
// Upgrade entry point
// ---------------------------------------------------------------------------

pub async fn ws_upgrade(ws: WebSocketUpgrade, State(st): State<AppState>) -> Response {
    ws.on_upgrade(move |socket| ws_handler(socket, st))
}

// ---------------------------------------------------------------------------
// Handler
// ---------------------------------------------------------------------------

pub async fn ws_handler(mut socket: WebSocket, st: AppState) {
    while let Some(Ok(msg)) = socket.next().await {
        let Message::Text(txt) = msg else { continue };

        let resp_txt = match serde_json::from_str::<WsCmd>(&txt) {
            Err(e) => serde_json::to_string(&WsResp::<serde_json::Value> {
                ok: false,
                cmd: "error".into(),
                data: None,
                error: Some(e.to_string()),
            })
            .unwrap(),

            Ok(cmd) => dispatch(cmd, &st).await,
        };

        let _ = socket.send(Message::Text(resp_txt.into())).await;
    }
}

// ---------------------------------------------------------------------------
// Dispatch
// ---------------------------------------------------------------------------

async fn dispatch(cmd: WsCmd, st: &AppState) -> String {
    let db = st.db.clone();

    match cmd {
        

        // ---- Globals ----
        WsCmd::Globals => match task::spawn_blocking(move || -> anyhow::Result<GlobalMetrics> {
            global_metrics(&db)
        })
        .await
        {
            Ok(Ok(g)) => ok("globals", g),
            Ok(Err(e)) => err("globals", e),
            Err(e) => err("globals", e),
        },

        // ---- Decode ----
        WsCmd::Decode { txid } => match task::spawn_blocking(move || decode_utxo_entry(&db, &txid)).await {
            Ok(Ok(d)) => ok("decode", d),
            Ok(Err(e)) => err("decode", e),
            Err(e) => err("decode", e),
        },

        // ---- Balance ----
        WsCmd::Balance { address } => match task::spawn_blocking(move || -> anyhow::Result<ApiBalanceResp> {
            let addr = Public::new_from_base36(&address)
                .ok_or_else(|| anyhow::anyhow!("invalid address base36"))?;
            let bal = confirmed_balance(&db, &addr)?;
            Ok(ApiBalanceResp {
                address,
                confirmed_balance: bal,
            })
        })
        .await
        {
            Ok(Ok(r)) => ok("balance", r),
            Ok(Err(e)) => err("balance", e),
            Err(e) => err("balance", e),
        },

        // ---- UtxosFor ----
        WsCmd::UtxosFor { address, limit } => {
            let lim = limit.unwrap_or(200);
            match task::spawn_blocking(move || -> anyhow::Result<Vec<String>> {
                let addr = Public::new_from_base36(&address)
                    .ok_or_else(|| anyhow::anyhow!("invalid address base36"))?;
                let mut out = Vec::new();
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
                        continue
                    };
                    if outputs.iter().any(|o| o.is_some_and(|o| o.receiver == addr)) {
                        out.push(txid_str);
                        if out.len() >= lim {
                            break;
                        }
                    }
                }
                Ok(out)
            })
            .await
            {
                Ok(Ok(r)) => ok("utxos_for", r),
                Ok(Err(e)) => err("utxos_for", e),
                Err(e) => err("utxos_for", e),
            }
        }

        // ---- TopReceivers ----
        WsCmd::TopReceivers { limit } => {
            let lim = limit.unwrap_or(9999);
            match task::spawn_blocking(move || top_receivers(&db, lim)).await {
                Ok(Ok(r)) => ok("top_receivers", r),
                Ok(Err(e)) => err("top_receivers", e),
                Err(e) => err("top_receivers", e),
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn ok<T: Serialize>(cmd: &str, data: T) -> String {
    serde_json::to_string(&WsResp {
        ok: true,
        cmd: cmd.to_string(),
        data: Some(data),
        error: None::<String>,
    })
    .unwrap()
}

fn err<E: std::fmt::Display>(cmd: &str, e: E) -> String {
    serde_json::to_string(&WsResp::<serde_json::Value> {
        ok: false,
        cmd: cmd.to_string(),
        data: None,
        error: Some(e.to_string()),
    })
    .unwrap()
}

// =============================================================================
// src/ws.rs
// snapcoin-db-inspector/src/ws.rs
// Updated: 2026-02-26T00:00:00Z
// =============================================================================