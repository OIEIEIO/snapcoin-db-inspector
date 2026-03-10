// =============================================================================
// src/main.rs
// snapcoin-db-inspector/src/main.rs
// v0.4.2-sentinel-cli-wireup.1
// Entry point: CLI parsing, AppState, Axum router, serve/cli dispatch.
//
// Change (v0.4.2 ... .1):
// - ADD top-level Sentinel mode
// - ADD sentinel subcommands:
//     ScanIndex
//     ScanRange
//     ScanAll
//     Watch
// - Existing Serve and Cli modes remain unchanged
// - No API route changes
// =============================================================================

mod api;
mod db;
mod events;
mod rules;
mod scanner;
mod sentinel;
mod ws;

use anyhow::{anyhow, Context, Result};
use axum::{routing::get, Router};
use clap::{Parser, Subcommand};
use std::{net::SocketAddr, sync::Arc};
use tower_http::services::ServeDir;

pub use db::AppState;

#[derive(Parser, Debug)]
#[command(name = "snapcoin-db-inspector")]
#[command(about = "Snap-Coin Sled inspector + explorer server", long_about = None)]
struct Cli {
    #[command(subcommand)]
    mode: Mode,
}

#[derive(Subcommand, Debug)]
enum Mode {
    /// Run the web explorer (HTML + JSON API + WebSocket)
    Serve {
        #[arg(long)]
        db: String,
        #[arg(long, default_value = "127.0.0.1:5337")]
        bind: String,
    },

    /// Run CLI commands (inspector / decoder)
    Cli {
        #[arg(long)]
        db: String,
        #[command(subcommand)]
        cmd: db::Cmd,
    },

    /// Run sentinel anomaly scans
    Sentinel {
        #[arg(long)]
        db: String,

        #[command(subcommand)]
        cmd: SentinelCmd,
    },
}

#[derive(Subcommand, Debug)]
enum SentinelCmd {
    /// Scan one indexed tx entry
    ScanIndex {
        #[arg(long)]
        index: u64,
    },

    /// Scan a range of indexed tx entries
    ScanRange {
        #[arg(long)]
        start: u64,
        #[arg(long)]
        end: u64,
    },

    /// Scan the full DB for tx / UTXO anomalies
    ScanAll,

    /// Watch mode: periodically re-scan the DB
    Watch {
        #[arg(long, default_value_t = 5)]
        poll_seconds: u64,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.mode {
        Mode::Serve { db, bind } => {
            let database =
                sled::open(&db).with_context(|| format!("open sled db at {db}"))?;
            let state = AppState {
                db: Arc::new(database),
            };

            let app = Router::new()
                // UI
                .route("/", get(api::ui_index))
                // Static assets (CSS/JS/HTML)
                .nest_service("/static", ServeDir::new("static"))
                // API — globals + balance + utxos
                .route("/api/globals", get(api::api_globals))
                .route("/api/get/{txid}", get(api::api_get_txid))
                .route("/api/decode/{txid}", get(api::api_decode_txid))
                .route("/api/utxos_for/{address}", get(api::api_utxos_for))
                .route("/api/balance/{address}", get(api::api_balance))
                .route("/api/top_receivers", get(api::api_top_receivers))
                // API — wallet intelligence
                .route("/api/wallet_intelligence", get(api::api_wallet_intelligence))
                .route("/api/wallet_stats/{address}", get(api::api_wallet_stats))
                // API — default-tree inspector endpoints
                .route("/api/inspect_default", get(api::api_inspect_default))
                .route("/api/prefix_stats", get(api::api_prefix_stats))
                .route("/api/preview_keys", get(api::api_preview_keys))
                // WebSocket
                .route("/ws", get(ws::ws_upgrade))
                .with_state(state);

            let addr: SocketAddr = bind.parse().map_err(|e| anyhow!("bad --bind: {e}"))?;
            println!("serving on http://{addr}");
            axum::serve(tokio::net::TcpListener::bind(addr).await?, app).await?;
            Ok(())
        }

        Mode::Cli { db, cmd } => {
            let database =
                sled::open(&db).with_context(|| format!("open sled db at {db}"))?;
            db::run_cli(database, cmd)?;
            Ok(())
        }

        Mode::Sentinel { db, cmd } => {
            let database =
                sled::open(&db).with_context(|| format!("open sled db at {db}"))?;

            sentinel::print_banner();

            match cmd {
                SentinelCmd::ScanIndex { index } => {
                    let cfg = sentinel::SentinelConfig::default();
                    sentinel::run_index_scan(&database, &cfg, index)?;
                }

                SentinelCmd::ScanRange { start, end } => {
                    let cfg = sentinel::SentinelConfig::default();
                    sentinel::run_index_range_scan(&database, &cfg, start, end)?;
                }

                SentinelCmd::ScanAll => {
                    let cfg = sentinel::SentinelConfig::default();
                    sentinel::run_full_tx_scan(&database, &cfg)?;
                }

                SentinelCmd::Watch { poll_seconds } => {
                    let cfg = sentinel::SentinelConfig {
                        poll_seconds,
                        ..Default::default()
                    };
                    sentinel::run_watch_loop(database, cfg).await?;
                }
            }

            Ok(())
        }
    }
}

// =============================================================================
// src/main.rs
// snapcoin-db-inspector/src/main.rs
// Created: 2026-02-26T00:00:00Z
// Version: v0.4.2-sentinel-cli-wireup.1
// =============================================================================