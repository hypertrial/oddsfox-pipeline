pub mod client;
pub mod models;
pub mod normalize;

use chrono::Utc;

use crate::config::{KalshiStatus, SnapshotBooksOptions, SyncMarketsOptions, SyncPricesOptions, Table};
use crate::error::{OddsfoxError, Result};
use crate::http::HttpClient;
use crate::manifest::{new_run_id, ManifestStore, RunRecord, SyncStateRecord};
use crate::parquet::{write_snapshot, write_token_series};
use crate::paths::LakePaths;
use crate::quarantine::{sha256_hex, write_raw_json};

use client::{KalshiAuth, KalshiClient};

fn client_from_parts(
    base_url: String,
    key_id: Option<String>,
    private_key_path: Option<std::path::PathBuf>,
    requests_per_second: f64,
    max_retries: u32,
    user_agent: String,
) -> Result<KalshiClient> {
    let http = HttpClient::new(requests_per_second, max_retries, user_agent)?;
    let auth = match (key_id, private_key_path) {
        (Some(key_id), Some(path)) => Some(KalshiAuth::from_key_file(key_id, &path)?),
        _ => None,
    };
    Ok(KalshiClient::new(base_url, http, auth))
}

pub async fn sync_markets(options: SyncMarketsOptions) -> Result<crate::sync::SyncSummary> {
    let paths = LakePaths::new(&options.out);
    let store = ManifestStore::open(&options.out)?;
    let run_id = new_run_id();
    let started = Utc::now();
    let client = client_from_parts(
        options.kalshi_rest_base_url.clone(),
        options.kalshi_key_id.clone(),
        options.kalshi_private_key_path.clone(),
        options.requests_per_second,
        options.max_retries,
        options.user_agent.clone(),
    )?;

    let status = options.status.unwrap_or({
        if options.all {
            KalshiStatus::All
        } else if options.closed {
            KalshiStatus::Closed
        } else {
            KalshiStatus::Open
        }
    });
    let response = if let Some(event_ticker) = options.event.as_deref() {
        let event = client.get_event(event_ticker).await?;
        models::KalshiMarketResponse {
            markets: event.markets,
            cursor: None,
        }
    } else {
        client
            .get_markets(status, options.series.as_deref(), options.limit)
            .await?
    };
    let markets = response.markets;
    let events = normalize::events_from_markets(&markets);
    let raw = serde_json::to_vec(&markets)?;
    let raw_sha = sha256_hex(&raw);
    let raw_url = client.markets_url(status, options.series.as_deref(), options.limit);
    write_raw_json(&paths, "kalshi", &format!("markets-{run_id}.json"), &raw)?;

    let events_data = normalize::events_batch(&events, &raw_url, &raw_sha, &run_id)?;
    let markets_data = normalize::markets_batch(&markets, &raw_url, &raw_sha, &run_id)?;
    let outcomes_data = normalize::outcomes_batch(&markets, &raw_url, &raw_sha, &run_id)?;
    let resolutions_data = normalize::resolutions_batch(&markets, &raw_url, &raw_sha, &run_id)?;

    write_snapshot(&paths, Table::Events, &run_id, &[events_data])?;
    write_snapshot(&paths, Table::Markets, &run_id, &[markets_data])?;
    write_snapshot(&paths, Table::Outcomes, &run_id, &[outcomes_data])?;
    if resolutions_data.num_rows() > 0 {
        write_snapshot(&paths, Table::Resolutions, &run_id, &[resolutions_data])?;
    }

    store.upsert_sync_state(SyncStateRecord {
        source: "kalshi".into(),
        cursor_key: "markets".into(),
        cursor_value: markets.len().to_string(),
        last_ts: Some(Utc::now()),
        updated_at: Utc::now(),
    })?;
    store.write_schema_records()?;
    crate::contract::refresh_contract(&paths)?;

    store.append_run(RunRecord {
        run_id: run_id.clone(),
        command: "sync markets --source kalshi".into(),
        started_at: started,
        finished_at: Some(Utc::now()),
        status: "complete".into(),
        rows_written: (events.len() + markets.len()) as i64,
        oddsfox_version: env!("CARGO_PKG_VERSION").into(),
    })?;

    println!(
        "sync kalshi markets complete: {} events, {} markets (run={run_id})",
        events.len(),
        markets.len()
    );
    Ok(crate::sync::SyncSummary {
        events: events.len(),
        markets: markets.len(),
        run_id,
    })
}

pub async fn sync_prices(options: SyncPricesOptions) -> Result<()> {
    let paths = LakePaths::new(&options.out);
    let store = ManifestStore::open(&options.out)?;
    let run_id = new_run_id();
    let started = Utc::now();
    let client = client_from_parts(
        options.kalshi_rest_base_url.clone(),
        options.kalshi_key_id.clone(),
        options.kalshi_private_key_path.clone(),
        options.requests_per_second,
        options.max_retries,
        options.user_agent.clone(),
    )?;
    let market_id = options.market_id.as_deref().ok_or_else(|| OddsfoxError::SyncIncomplete {
        message: "pass --market <kalshi_ticker> for Kalshi price sync".into(),
    })?;
    let ticker = normalize::strip_kalshi_market_id(market_id);
    let series = options.series.clone().or_else(|| infer_series(ticker)).ok_or_else(|| {
        OddsfoxError::SyncIncomplete {
            message: "pass --series <series_ticker> for Kalshi candlesticks".into(),
        }
    })?;
    let start_ts = options
        .since
        .map(|d| d.and_hms_opt(0, 0, 0).unwrap().and_utc().timestamp());
    let end_ts = options
        .until
        .map(|d| d.and_hms_opt(23, 59, 59).unwrap().and_utc().timestamp());
    let period = options.period.or(options.fidelity).unwrap_or(60);
    let candles = client
        .get_candlesticks(&series, ticker, period, start_ts, end_ts)
        .await?;
    let (yes, no, skipped) = normalize::price_points_from_candlesticks(&candles);
    let market = normalize::kalshi_market_id(ticker);
    let yes_token = normalize::kalshi_token_id(ticker, "yes");
    let no_token = normalize::kalshi_token_id(ticker, "no");
    let fidelity = Some(period as i32);
    let mut rows = 0;
    if !yes.is_empty() {
        let batch = crate::normalize::prices_batch(
            &yes_token,
            Some(&market),
            &yes,
            "kalshi_candlesticks",
            fidelity,
            &run_id,
        )?;
        rows += batch.num_rows() as i64;
        write_token_series(&paths, Table::Prices, &yes_token, &[batch])?;
    }
    if !no.is_empty() {
        let batch = crate::normalize::prices_batch(
            &no_token,
            Some(&market),
            &no,
            "kalshi_candlesticks",
            fidelity,
            &run_id,
        )?;
        rows += batch.num_rows() as i64;
        write_token_series(&paths, Table::Prices, &no_token, &[batch])?;
    }

    store.append_run(RunRecord {
        run_id: run_id.clone(),
        command: "sync prices --source kalshi".into(),
        started_at: started,
        finished_at: Some(Utc::now()),
        status: "complete".into(),
        rows_written: rows,
        oddsfox_version: env!("CARGO_PKG_VERSION").into(),
    })?;
    println!("sync kalshi prices complete: {rows} points, {skipped} skipped (run={run_id})");
    Ok(())
}

pub async fn sync_trades(options: SyncPricesOptions) -> Result<()> {
    let paths = LakePaths::new(&options.out);
    let store = ManifestStore::open(&options.out)?;
    let run_id = new_run_id();
    let started = Utc::now();
    let client = client_from_parts(
        options.kalshi_rest_base_url.clone(),
        options.kalshi_key_id.clone(),
        options.kalshi_private_key_path.clone(),
        options.requests_per_second,
        options.max_retries,
        options.user_agent.clone(),
    )?;
    let market_id = options.market_id.as_deref().ok_or_else(|| OddsfoxError::SyncIncomplete {
        message: "pass --market <kalshi_ticker> for Kalshi trade sync".into(),
    })?;
    let ticker = normalize::strip_kalshi_market_id(market_id);
    let start_ts = options
        .since
        .map(|d| d.and_hms_opt(0, 0, 0).unwrap().and_utc().timestamp());
    let end_ts = options
        .until
        .map(|d| d.and_hms_opt(23, 59, 59).unwrap().and_utc().timestamp());
    let trades = client.get_trades(Some(ticker), start_ts, end_ts, options.limit).await?;
    if trades.is_empty() {
        println!("sync kalshi trades: no trades selected");
        return Ok(());
    }
    let batch = normalize::trades_batch(&trades, &run_id)?;
    let rows = batch.num_rows() as i64;
    write_snapshot(&paths, Table::Trades, &run_id, &[batch])?;
    store.append_run(RunRecord {
        run_id: run_id.clone(),
        command: "sync trades --source kalshi".into(),
        started_at: started,
        finished_at: Some(Utc::now()),
        status: "complete".into(),
        rows_written: rows,
        oddsfox_version: env!("CARGO_PKG_VERSION").into(),
    })?;
    println!("sync kalshi trades complete: {rows} trades (run={run_id})");
    Ok(())
}

pub async fn snapshot_books(options: SnapshotBooksOptions) -> Result<()> {
    let paths = LakePaths::new(&options.out);
    let store = ManifestStore::open(&options.out)?;
    let run_id = new_run_id();
    let started = Utc::now();
    let client = client_from_parts(
        options.kalshi_rest_base_url.clone(),
        options.kalshi_key_id.clone(),
        options.kalshi_private_key_path.clone(),
        options.requests_per_second,
        options.max_retries,
        options.user_agent.clone(),
    )?;
    let market_id = options.market_id.as_deref().ok_or_else(|| OddsfoxError::SyncIncomplete {
        message: "pass --market <kalshi_ticker> for Kalshi orderbook snapshots".into(),
    })?;
    let ticker = normalize::strip_kalshi_market_id(market_id);
    let book = client.get_orderbook(ticker, options.depth).await?;
    let records = normalize::snapshot_records_from_orderbook(ticker, &book);
    if records.is_empty() {
        println!("snapshot kalshi books: no levels");
        return Ok(());
    }
    let books_batch = crate::normalize::orderbooks_batch(&records, "kalshi_orderbook", &run_id)?;
    let levels_batch = crate::normalize::book_levels_batch(&records, "kalshi_orderbook", &run_id)?;
    write_snapshot(&paths, Table::Orderbooks, &run_id, &[books_batch])?;
    write_snapshot(&paths, Table::BookLevels, &run_id, &[levels_batch])?;
    store.append_run(RunRecord {
        run_id: run_id.clone(),
        command: "snapshot books --source kalshi".into(),
        started_at: started,
        finished_at: Some(Utc::now()),
        status: "complete".into(),
        rows_written: records.len() as i64,
        oddsfox_version: env!("CARGO_PKG_VERSION").into(),
    })?;
    println!("snapshot kalshi books complete: {} snapshots (run={run_id})", records.len());
    Ok(())
}

pub async fn historical_cutoff(options: &SyncMarketsOptions) -> Result<Option<i64>> {
    let client = client_from_parts(
        options.kalshi_rest_base_url.clone(),
        options.kalshi_key_id.clone(),
        options.kalshi_private_key_path.clone(),
        options.requests_per_second,
        options.max_retries,
        options.user_agent.clone(),
    )?;
    client.get_historical_cutoff().await
}

fn infer_series(ticker: &str) -> Option<String> {
    ticker.split('-').next().map(str::to_string).filter(|s| !s.is_empty())
}
