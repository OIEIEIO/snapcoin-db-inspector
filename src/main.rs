// =============================================================================
// src/main.rs
// snapcoin-db-inspector/src/main.rs
// v0.4.0-wallet-intelligence.1
// Entry point: CLI parsing, AppState, Axum router, serve/cli dispatch.
//
// Change (v0.4.0 ... .1):
// - ADD HTTP routes for wallet intelligence endpoints:
//     GET /api/wallet_intelligence
//     GET /api/wallet_stats/:address
// - No structural changes. CLI subcommand wiring unchanged —
//   new db::Cmd variants (WalletIntelligence, WalletStats) route
//   automatically via db::run_cli().
// =============================================================================

mod api;
mod db;
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
                .route("/api/globals",              get(api::api_globals))
                .route("/api/get/{txid}",           get(api::api_get_txid))
                .route("/api/decode/{txid}",        get(api::api_decode_txid))
                .route("/api/utxos_for/{address}",  get(api::api_utxos_for))
                .route("/api/balance/{address}",    get(api::api_balance))
                .route("/api/top_receivers",        get(api::api_top_receivers))
                // API — wallet intelligence
                .route("/api/wallet_intelligence",          get(api::api_wallet_intelligence))
                .route("/api/wallet_stats/{address}",       get(api::api_wallet_stats))
                // API — default-tree inspector endpoints
                .route("/api/inspect_default",  get(api::api_inspect_default))
                .route("/api/prefix_stats",     get(api::api_prefix_stats))
                .route("/api/preview_keys",     get(api::api_preview_keys))
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
    }
}

// =============================================================================
// src/main.rs
// snapcoin-db-inspector/src/main.rs
// Created: 2026-02-26T00:00:00Z
// =============================================================================
