use std::path::PathBuf;

use clap::{Parser, Subcommand, ValueEnum};

use crate::config::{BackfillSource, KalshiStatus, LakeOptions, Source, TopBy};
use crate::error::Result;
use crate::settings::{resolve_config, OddsfoxConfig};

#[derive(Parser, Debug)]
#[command(name = "oddsfox", version, about = "Self-hosted prediction-market data lake creator")]
pub struct Cli {
    #[arg(long, global = true)]
    pub config: Option<PathBuf>,
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand, Debug)]
pub enum Commands {
    Init {
        #[arg(long)]
        out: Option<PathBuf>,
    },
    Quickstart {
        #[arg(long)]
        out: Option<PathBuf>,
        #[arg(long, help = "Ignored for compatibility; serve reads Parquet directly")]
        db: Option<PathBuf>,
        #[arg(long, default_value_t = 8787)]
        port: u16,
        #[arg(long, default_value_t = 50)]
        top_volume: usize,
    },
    Backfill {
        #[arg(long, value_enum, default_value_t = BackfillSource::Polymarket)]
        source: BackfillSource,
        #[arg(long)]
        out: Option<PathBuf>,
        #[arg(long)]
        db: Option<PathBuf>,
        #[arg(long, default_value_t = false)]
        active: bool,
        #[arg(long, default_value_t = false)]
        closed: bool,
        #[arg(long, default_value_t = true)]
        all: bool,
        #[arg(long)]
        tag: Option<String>,
        #[arg(long)]
        limit: Option<usize>,
        #[arg(long)]
        fidelity: Option<u32>,
        #[arg(long)]
        interval: Option<String>,
        #[arg(long)]
        since: Option<String>,
        #[arg(long)]
        until: Option<String>,
        #[arg(long)]
        recent_hours: Option<u32>,
        #[arg(long)]
        rps: Option<f64>,
        #[arg(long)]
        concurrency: Option<usize>,
        #[arg(long, default_value_t = false)]
        overwrite: bool,
        #[arg(long, default_value_t = 8787)]
        port: u16,
    },
    Sync {
        #[command(subcommand)]
        target: SyncCommands,
    },
    Snapshot {
        #[command(subcommand)]
        target: SnapshotCommands,
    },
    Watch {
        #[arg(long)]
        out: Option<PathBuf>,
        #[arg(long)]
        market: Option<String>,
        #[arg(long, default_value_t = false)]
        active: bool,
        #[arg(long)]
        tag: Option<String>,
        #[arg(long)]
        top_volume: Option<usize>,
    },
    Compute {
        #[command(subcommand)]
        target: ComputeCommands,
    },
    Search {
        query: String,
        #[arg(long)]
        out: Option<PathBuf>,
    },
    Market {
        market_id: String,
        #[arg(long)]
        out: Option<PathBuf>,
    },
    Event {
        event_id: String,
        #[arg(long)]
        out: Option<PathBuf>,
    },
    Resolved {
        #[arg(long)]
        out: Option<PathBuf>,
        #[arg(long)]
        since: Option<String>,
    },
    Top {
        #[arg(long, value_enum, default_value_t = TopByArg::Volume24h)]
        by: TopByArg,
        #[arg(long)]
        out: Option<PathBuf>,
        #[arg(long, default_value_t = 25)]
        limit: usize,
    },
    Metrics {
        #[command(subcommand)]
        target: MetricsCommands,
    },
    Check {
        #[arg(long)]
        out: Option<PathBuf>,
    },
    Repair {
        #[arg(long)]
        out: Option<PathBuf>,
    },
    Clean {
        #[arg(long)]
        out: Option<PathBuf>,
        #[arg(long, default_value_t = false)]
        dry_run: bool,
    },
    Schema {
        table: String,
    },
    Contract {
        #[arg(long)]
        out: Option<PathBuf>,
    },
    Duckdb {
        #[arg(long)]
        out: Option<PathBuf>,
        #[arg(long)]
        db: Option<PathBuf>,
    },
    Sql {
        query: String,
        #[arg(long)]
        out: Option<PathBuf>,
        #[arg(long)]
        db: Option<PathBuf>,
    },
    Serve {
        #[arg(long, default_value_t = 8787)]
        port: u16,
        #[arg(long)]
        out: Option<PathBuf>,
        #[arg(long)]
        db: Option<PathBuf>,
    },
    Stats {
        #[arg(long)]
        out: Option<PathBuf>,
    },
    Head {
        #[arg(long)]
        out: Option<PathBuf>,
        #[arg(long)]
        export_dir: Option<PathBuf>,
        #[arg(long, default_value_t = 30)]
        limit: usize,
    },
}

#[derive(Subcommand, Debug)]
pub enum SyncCommands {
    Markets {
        #[arg(long, value_enum, default_value_t = Source::Polymarket)]
        source: Source,
        #[arg(long, value_enum)]
        status: Option<KalshiStatus>,
        #[arg(long)]
        series: Option<String>,
        #[arg(long)]
        event: Option<String>,
        #[arg(long, default_value_t = false)]
        active: bool,
        #[arg(long, default_value_t = false)]
        closed: bool,
        #[arg(long, default_value_t = false)]
        all: bool,
        #[arg(long)]
        tag: Option<String>,
        #[arg(long)]
        since: Option<String>,
        #[arg(long)]
        limit: Option<usize>,
        #[arg(long)]
        out: Option<PathBuf>,
    },
    Prices {
        #[arg(long, value_enum, default_value_t = Source::Polymarket)]
        source: Source,
        #[arg(long)]
        market: Option<String>,
        #[arg(long)]
        series: Option<String>,
        #[arg(long, default_value_t = false)]
        active: bool,
        #[arg(long, default_value_t = false)]
        all: bool,
        #[arg(long)]
        tag: Option<String>,
        #[arg(long)]
        limit: Option<usize>,
        #[arg(long)]
        top_limit: Option<usize>,
        #[arg(long)]
        interval: Option<String>,
        #[arg(long)]
        fidelity: Option<u32>,
        #[arg(long)]
        period: Option<u32>,
        #[arg(long)]
        since: Option<String>,
        #[arg(long)]
        until: Option<String>,
        #[arg(long)]
        recent_hours: Option<u32>,
        #[arg(long, default_value_t = false)]
        overwrite: bool,
        #[arg(long)]
        rps: Option<f64>,
        #[arg(long)]
        concurrency: Option<usize>,
        #[arg(long)]
        out: Option<PathBuf>,
    },
    Trades {
        #[arg(long, value_enum, default_value_t = Source::Kalshi)]
        source: Source,
        #[arg(long)]
        market: Option<String>,
        #[arg(long)]
        since: Option<String>,
        #[arg(long)]
        until: Option<String>,
        #[arg(long)]
        limit: Option<usize>,
        #[arg(long)]
        rps: Option<f64>,
        #[arg(long)]
        out: Option<PathBuf>,
    },
}

#[derive(Subcommand, Debug)]
pub enum SnapshotCommands {
    Books {
        #[arg(long, value_enum, default_value_t = Source::Polymarket)]
        source: Source,
        #[arg(long)]
        market: Option<String>,
        #[arg(long, default_value_t = false)]
        active: bool,
        #[arg(long)]
        top_volume: Option<usize>,
        #[arg(long)]
        tokens: Option<PathBuf>,
        #[arg(long)]
        depth: Option<u32>,
        #[arg(long)]
        out: Option<PathBuf>,
    },
}

#[derive(Subcommand, Debug)]
pub enum ComputeCommands {
    Liquidity {
        #[arg(long, default_value_t = true)]
        active: bool,
        #[arg(long)]
        out: Option<PathBuf>,
    },
    Accuracy {
        #[arg(long, default_value_t = true)]
        resolved: bool,
        #[arg(long)]
        since: Option<String>,
        #[arg(long)]
        out: Option<PathBuf>,
    },
    Calibration {
        #[arg(long, default_value_t = true)]
        resolved: bool,
        #[arg(long, default_value_t = 0.05)]
        bucket_width: f64,
        #[arg(long)]
        since: Option<String>,
        #[arg(long)]
        out: Option<PathBuf>,
    },
    All {
        #[arg(long)]
        since: Option<String>,
        #[arg(long)]
        out: Option<PathBuf>,
    },
}

#[derive(Subcommand, Debug)]
pub enum MetricsCommands {
    Market {
        market_id: String,
        #[arg(long)]
        out: Option<PathBuf>,
    },
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum TopByArg {
    Volume24h,
    Spread,
    Liquidity,
    Volume,
}

impl From<TopByArg> for TopBy {
    fn from(value: TopByArg) -> Self {
        match value {
            TopByArg::Volume24h => TopBy::Volume24h,
            TopByArg::Spread => TopBy::Spread,
            TopByArg::Liquidity => TopBy::Liquidity,
            TopByArg::Volume => TopBy::Volume,
        }
    }
}

impl Cli {
    pub fn lake_root(&self) -> Result<PathBuf> {
        lake_root_from_config(self.config.as_deref(), None)
    }

    pub fn lake_options(&self, out: Option<PathBuf>) -> Result<LakeOptions> {
        Ok(LakeOptions {
            out: lake_root_from_config(self.config.as_deref(), out)?,
        })
    }
}

pub fn lake_root_from_config(config: Option<&std::path::Path>, out: Option<PathBuf>) -> Result<PathBuf> {
    if let Some(out) = out {
        return Ok(out);
    }
    let cfg = resolve_config(config, None)?;
    Ok(PathBuf::from(cfg.data.home))
}

pub fn default_db_for(out: &std::path::Path, config: &OddsfoxConfig) -> PathBuf {
    let db = PathBuf::from(&config.duckdb.database);
    if db.is_absolute() {
        db
    } else {
        out.join(db)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_kalshi_sync_commands() {
        Cli::try_parse_from([
            "oddsfox", "sync", "markets", "--source", "kalshi", "--status", "open",
            "--series", "KXTEST", "--limit", "2",
        ])
        .unwrap();
        Cli::try_parse_from([
            "oddsfox", "sync", "prices", "--source", "kalshi", "--market", "KXTEST-26",
            "--series", "KXTEST", "--period", "60",
        ])
        .unwrap();
        Cli::try_parse_from([
            "oddsfox", "sync", "trades", "--source", "kalshi", "--market", "KXTEST-26",
        ])
        .unwrap();
    }

    #[test]
    fn parses_kalshi_snapshot_and_backfill() {
        Cli::try_parse_from([
            "oddsfox", "snapshot", "books", "--source", "kalshi", "--market", "KXTEST-26",
            "--depth", "10",
        ])
        .unwrap();
        Cli::try_parse_from(["oddsfox", "backfill", "--source", "all"]).unwrap();
        Cli::try_parse_from([
            "oddsfox", "sync", "prices", "--source", "kalshi", "--active", "--recent-hours", "24",
        ])
        .unwrap();
    }
}
