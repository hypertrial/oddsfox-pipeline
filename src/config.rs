use std::path::PathBuf;
use std::str::FromStr;

use chrono::NaiveDate;
use clap::ValueEnum;

use crate::error::{OddsfoxError, Result};

pub const DEFAULT_GAMMA_BASE_URL: &str = "https://gamma-api.polymarket.com";
pub const DEFAULT_CLOB_BASE_URL: &str = "https://clob.polymarket.com";
pub const DEFAULT_DATA_BASE_URL: &str = "https://data-api.polymarket.com";
pub const DEFAULT_WS_MARKET_URL: &str = "wss://ws-subscriptions-clob.polymarket.com/ws/market";
pub const DEFAULT_KALSHI_REST_BASE_URL: &str = "https://external-api.kalshi.com/trade-api/v2";
pub const DEFAULT_KALSHI_WS_URL: &str = "wss://external-api-ws.kalshi.com/trade-api/ws/v2";
pub const DEFAULT_REQUESTS_PER_SECOND: f64 = 2.0;
pub const DEFAULT_MAX_RETRIES: u32 = 5;
pub const DEFAULT_USER_AGENT: &str = "oddsfox/0.2.0";
pub const DEFAULT_RAW_RETENTION_DAYS: u32 = 30;
pub const DEFAULT_BACKFILL_REQUESTS_PER_SECOND: f64 = 5.0;
pub const DEFAULT_BACKFILL_CONCURRENCY: usize = 4;
pub const DEFAULT_BACKFILL_FIDELITY_MINUTES: u32 = 60;
pub const DEFAULT_BACKFILL_INTERVAL: &str = "max";
pub const DEFAULT_ACTIVE_PRICE_LIMIT: usize = 100;
pub const BATCH_TOKEN_LIMIT: usize = 500;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, ValueEnum)]
pub enum Table {
    Events,
    Markets,
    Outcomes,
    Prices,
    Orderbooks,
    BookLevels,
    Trades,
    Resolutions,
}

impl Table {
    pub fn as_str(self) -> &'static str {
        match self {
            Table::Events => "events",
            Table::Markets => "markets",
            Table::Outcomes => "outcomes",
            Table::Prices => "prices",
            Table::Orderbooks => "orderbooks",
            Table::BookLevels => "book_levels",
            Table::Trades => "trades",
            Table::Resolutions => "resolutions",
        }
    }

    pub fn all() -> &'static [Table] {
        &[
            Table::Events,
            Table::Markets,
            Table::Outcomes,
            Table::Prices,
            Table::Orderbooks,
            Table::BookLevels,
            Table::Trades,
            Table::Resolutions,
        ]
    }

    pub fn is_time_series(self) -> bool {
        matches!(
            self,
            Table::Prices | Table::Orderbooks | Table::BookLevels | Table::Trades
        )
    }
}

impl FromStr for Table {
    type Err = OddsfoxError;

    fn from_str(s: &str) -> Result<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "events" | "event" => Ok(Table::Events),
            "markets" | "market" => Ok(Table::Markets),
            "outcomes" | "outcome" => Ok(Table::Outcomes),
            "prices" | "price" => Ok(Table::Prices),
            "orderbooks" | "orderbook" => Ok(Table::Orderbooks),
            "book_levels" | "book-levels" => Ok(Table::BookLevels),
            "trades" | "trade" => Ok(Table::Trades),
            "resolutions" | "resolution" => Ok(Table::Resolutions),
            other => Err(OddsfoxError::InvalidTable(other.to_string())),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum OutputFormat {
    Text,
    Json,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum Source {
    Polymarket,
    Kalshi,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum BackfillSource {
    Polymarket,
    Kalshi,
    All,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum KalshiStatus {
    Open,
    Closed,
    Settled,
    All,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum TopBy {
    Volume24h,
    Spread,
    Liquidity,
    Volume,
}

pub fn parse_date(raw: &str) -> Result<NaiveDate> {
    NaiveDate::parse_from_str(raw, "%Y-%m-%d")
        .map_err(|_| OddsfoxError::InvalidDate(raw.to_string()))
}

#[derive(Debug, Clone)]
pub struct LakeOptions {
    pub out: PathBuf,
}

#[derive(Debug, Clone)]
pub struct DuckDbOptions {
    pub out: PathBuf,
    pub db: PathBuf,
}

#[derive(Debug, Clone)]
pub struct SyncMarketsOptions {
    pub out: PathBuf,
    pub source: Source,
    pub active: bool,
    pub closed: bool,
    pub all: bool,
    pub status: Option<KalshiStatus>,
    pub series: Option<String>,
    pub event: Option<String>,
    pub tag: Option<String>,
    pub since: Option<NaiveDate>,
    pub limit: Option<usize>,
    pub resume: bool,
    pub overwrite: bool,
    pub gamma_base_url: String,
    pub kalshi_rest_base_url: String,
    pub kalshi_key_id: Option<String>,
    pub kalshi_private_key_path: Option<PathBuf>,
    pub requests_per_second: f64,
    pub max_retries: u32,
    pub user_agent: String,
    pub raw_retention_days: u32,
}

#[derive(Debug, Clone)]
pub struct SyncPricesOptions {
    pub out: PathBuf,
    pub source: Source,
    pub market_id: Option<String>,
    pub series: Option<String>,
    pub active: bool,
    pub all: bool,
    pub filter_active: Option<bool>,
    pub tag: Option<String>,
    pub limit: Option<usize>,
    pub top_limit: Option<usize>,
    pub interval: Option<String>,
    pub fidelity: Option<u32>,
    pub period: Option<u32>,
    pub since: Option<NaiveDate>,
    pub until: Option<NaiveDate>,
    pub overwrite: bool,
    pub concurrency: usize,
    pub clob_base_url: String,
    pub kalshi_rest_base_url: String,
    pub kalshi_key_id: Option<String>,
    pub kalshi_private_key_path: Option<PathBuf>,
    pub requests_per_second: f64,
    pub max_retries: u32,
    pub user_agent: String,
}

#[derive(Debug, Clone)]
pub struct SnapshotBooksOptions {
    pub out: PathBuf,
    pub source: Source,
    pub market_id: Option<String>,
    pub active: bool,
    pub top_volume: Option<usize>,
    pub tokens_file: Option<PathBuf>,
    pub depth: Option<u32>,
    pub clob_base_url: String,
    pub kalshi_rest_base_url: String,
    pub kalshi_key_id: Option<String>,
    pub kalshi_private_key_path: Option<PathBuf>,
    pub requests_per_second: f64,
    pub max_retries: u32,
    pub user_agent: String,
}

#[derive(Debug, Clone)]
pub struct WatchOptions {
    pub out: PathBuf,
    pub market_id: Option<String>,
    pub active: bool,
    pub tag: Option<String>,
    pub top_volume: Option<usize>,
    pub ws_url: String,
    pub clob_base_url: String,
}

#[derive(Debug, Clone)]
pub struct ComputeOptions {
    pub out: PathBuf,
    pub active: bool,
    pub resolved: bool,
    pub since: Option<NaiveDate>,
    pub bucket_width: f64,
}

#[derive(Debug, Clone)]
pub struct ServeOptions {
    pub out: PathBuf,
    pub port: u16,
    pub db: PathBuf,
}

#[derive(Debug, Clone)]
pub struct QuickstartOptions {
    pub out: PathBuf,
    pub db: PathBuf,
    pub port: u16,
    pub top_volume: usize,
}

#[derive(Debug, Clone)]
pub struct BackfillOptions {
    pub out: PathBuf,
    pub db: PathBuf,
    pub source: BackfillSource,
    pub active: bool,
    pub closed: bool,
    pub all: bool,
    pub tag: Option<String>,
    pub limit: Option<usize>,
    pub interval: Option<String>,
    pub fidelity: Option<u32>,
    pub since: Option<NaiveDate>,
    pub until: Option<NaiveDate>,
    pub requests_per_second: f64,
    pub concurrency: usize,
    pub overwrite: bool,
    pub max_retries: u32,
    pub user_agent: String,
    pub gamma_base_url: String,
    pub clob_base_url: String,
    pub kalshi_rest_base_url: String,
    pub kalshi_key_id: Option<String>,
    pub kalshi_private_key_path: Option<PathBuf>,
    pub raw_retention_days: u32,
    pub port: u16,
}

#[derive(Debug, Clone, Default)]
pub struct TokenPairFilter {
    pub active: Option<bool>,
    pub tag: Option<String>,
    pub limit: Option<usize>,
}
