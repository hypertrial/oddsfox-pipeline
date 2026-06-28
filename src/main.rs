use std::path::PathBuf;

use clap::Parser;
use tracing_subscriber::EnvFilter;

use oddsfox::cli::{
    lake_root_from_config, Cli, Commands, ComputeCommands, MetricsCommands, SnapshotCommands,
    SyncCommands,
};
use oddsfox::config::{
    parse_date, Table, TopBy, DEFAULT_BACKFILL_CONCURRENCY, DEFAULT_BACKFILL_FIDELITY_MINUTES,
    DEFAULT_BACKFILL_INTERVAL, DEFAULT_BACKFILL_REQUESTS_PER_SECOND,
};
use oddsfox::error::Result;
use oddsfox::duckdb::default_db_for_lake;
use oddsfox::settings::resolve_config;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::from_default_env().add_directive("oddsfox=info".parse().unwrap()),
        )
        .init();

    let cli = Cli::parse();
    let config = resolve_config(cli.config.as_deref(), None)?;

    match &cli.command {
        Commands::Init { out } => {
            let root = out.clone().unwrap_or_else(|| PathBuf::from(&config.data.home));
            oddsfox::init::run(&root)?;
        }
        Commands::Quickstart { out, db, port, top_volume } => {
            let root = out.clone().unwrap_or_else(|| PathBuf::from(&config.data.home));
            oddsfox::quickstart::run(oddsfox::config::QuickstartOptions {
                out: root,
                db: db.clone().unwrap_or_else(|| {
                    let root = out.clone().unwrap_or_else(|| PathBuf::from(&config.data.home));
                    default_db_for_lake(&oddsfox::paths::LakePaths::new(&root))
                }),
                port: *port,
                top_volume: *top_volume,
            })
            .await?;
        }
        Commands::Backfill {
            out,
            db,
            active,
            closed,
            all,
            tag,
            limit,
            fidelity,
            interval,
            since,
            until,
            rps,
            concurrency,
            overwrite,
            port,
        } => {
            let root = lake_root_from_config(cli.config.as_deref(), out.clone())?;
            let db_path = db.clone().unwrap_or_else(|| {
                default_db_for_lake(&oddsfox::paths::LakePaths::new(&root))
            });
            oddsfox::backfill::run(oddsfox::config::BackfillOptions {
                out: root,
                db: db_path,
                active: *active,
                closed: *closed,
                all: *all,
                tag: tag.clone(),
                limit: *limit,
                interval: interval
                    .clone()
                    .or_else(|| config.backfill.interval.clone())
                    .or_else(|| Some(DEFAULT_BACKFILL_INTERVAL.to_string())),
                fidelity: fidelity.or(config.backfill.fidelity_minutes).or(Some(
                    DEFAULT_BACKFILL_FIDELITY_MINUTES,
                )),
                since: since.as_ref().map(|s| parse_date(s)).transpose()?,
                until: until.as_ref().map(|s| parse_date(s)).transpose()?,
                requests_per_second: rps
                    .or(config.backfill.requests_per_second)
                    .unwrap_or(DEFAULT_BACKFILL_REQUESTS_PER_SECOND),
                concurrency: concurrency
                    .or(config.backfill.concurrency)
                    .unwrap_or(DEFAULT_BACKFILL_CONCURRENCY),
                overwrite: *overwrite,
                max_retries: config.sync.max_retries,
                user_agent: config.sync.user_agent.clone(),
                gamma_base_url: config.polymarket.gamma_base_url.clone(),
                clob_base_url: config.polymarket.clob_base_url.clone(),
                raw_retention_days: config.data.raw_retention_days,
                port: *port,
            })
            .await?;
        }
        Commands::Sync { target } => match target {
            SyncCommands::Markets {
                active,
                closed,
                all,
                tag,
                since,
                limit,
                out,
            } => {
                let root = lake_root_from_config(cli.config.as_deref(), out.clone())?;
                oddsfox::sync::sync_markets(oddsfox::config::SyncMarketsOptions {
                    out: root,
                    active: *active || (!*closed && !*all),
                    closed: *closed,
                    all: *all,
                    tag: tag.clone(),
                    since: since.as_ref().map(|s| parse_date(s)).transpose()?,
                    limit: *limit,
                    resume: true,
                    overwrite: false,
                    gamma_base_url: config.polymarket.gamma_base_url.clone(),
                    requests_per_second: config.sync.requests_per_second,
                    max_retries: config.sync.max_retries,
                    user_agent: config.sync.user_agent.clone(),
                    raw_retention_days: config.data.raw_retention_days,
                })
                .await?;
            }
            SyncCommands::Prices {
                market,
                active,
                all,
                tag,
                limit,
                top_limit,
                interval,
                fidelity,
                since,
                until,
                overwrite,
                rps,
                concurrency,
                out,
            } => {
                let root = lake_root_from_config(cli.config.as_deref(), out.clone())?;
                let filter_active = if *all {
                    if *active {
                        Some(true)
                    } else {
                        None
                    }
                } else {
                    None
                };
                oddsfox::prices::sync_prices(oddsfox::config::SyncPricesOptions {
                    out: root,
                    market_id: market.clone(),
                    active: *active && !*all,
                    all: *all,
                    filter_active,
                    tag: tag.clone(),
                    limit: *limit,
                    top_limit: *top_limit,
                    interval: interval.clone(),
                    fidelity: *fidelity,
                    since: since.as_ref().map(|s| parse_date(s)).transpose()?,
                    until: until.as_ref().map(|s| parse_date(s)).transpose()?,
                    overwrite: *overwrite,
                    concurrency: concurrency.unwrap_or(1),
                    clob_base_url: config.polymarket.clob_base_url.clone(),
                    requests_per_second: rps.unwrap_or(config.sync.requests_per_second),
                    max_retries: config.sync.max_retries,
                    user_agent: config.sync.user_agent.clone(),
                })
                .await?;
            }
        },
        Commands::Snapshot { target } => match target {
            SnapshotCommands::Books {
                market,
                active,
                top_volume,
                tokens,
                out,
            } => {
                let root = lake_root_from_config(cli.config.as_deref(), out.clone())?;
                oddsfox::snapshot::snapshot_books(oddsfox::config::SnapshotBooksOptions {
                    out: root,
                    market_id: market.clone(),
                    active: *active,
                    top_volume: *top_volume,
                    tokens_file: tokens.clone(),
                    clob_base_url: config.polymarket.clob_base_url.clone(),
                    requests_per_second: config.sync.requests_per_second,
                    max_retries: config.sync.max_retries,
                    user_agent: config.sync.user_agent.clone(),
                })
                .await?;
            }
        },
        Commands::Watch {
            out,
            market,
            active,
            tag,
            top_volume,
        } => {
            let root = lake_root_from_config(cli.config.as_deref(), out.clone())?;
            oddsfox::clob::watch_markets(oddsfox::config::WatchOptions {
                out: root,
                market_id: market.clone(),
                active: *active,
                tag: tag.clone(),
                top_volume: *top_volume,
                ws_url: config.polymarket.ws_market_url.clone(),
                clob_base_url: config.polymarket.clob_base_url.clone(),
            })
            .await?;
        }
        Commands::Compute { target } => {
            let compute = |out: &PathBuf, active: bool, resolved: bool, since: Option<String>, bucket_width: f64| -> Result<oddsfox::config::ComputeOptions> {
                Ok(oddsfox::config::ComputeOptions {
                    out: out.clone(),
                    active,
                    resolved,
                    since: since.as_ref().map(|s| parse_date(s)).transpose()?,
                    bucket_width,
                })
            };
            match target {
                ComputeCommands::Liquidity { active, out } => {
                    let root = lake_root_from_config(cli.config.as_deref(), out.clone())?;
                    oddsfox::metrics::compute_liquidity(&compute(&root, *active, false, None, 0.05)?).await?;
                }
                ComputeCommands::Accuracy { resolved, since, out } => {
                    let root = lake_root_from_config(cli.config.as_deref(), out.clone())?;
                    oddsfox::metrics::compute_accuracy(&compute(&root, false, *resolved, since.clone(), 0.05)?).await?;
                }
                ComputeCommands::Calibration { resolved: _, bucket_width, since, out } => {
                    let root = lake_root_from_config(cli.config.as_deref(), out.clone())?;
                    oddsfox::metrics::compute_calibration_metrics(&compute(&root, false, true, since.clone(), *bucket_width)?).await?;
                }
                ComputeCommands::All { since, out } => {
                    let root = lake_root_from_config(cli.config.as_deref(), out.clone())?;
                    oddsfox::metrics::compute_all(compute(&root, true, true, since.clone(), 0.05)?).await?;
                }
            }
        }
        Commands::Search { query, out } => {
            let root = lake_root_from_config(cli.config.as_deref(), out.clone())?;
            let hits = oddsfox::explore::search(&root, query)?;
            println!("{}", serde_json::to_string_pretty(&hits)?);
        }
        Commands::Market { market_id, out } => {
            let root = lake_root_from_config(cli.config.as_deref(), out.clone())?;
            let detail = oddsfox::explore::market_detail(&root, market_id)?;
            println!("{}", serde_json::to_string_pretty(&detail)?);
        }
        Commands::Event { event_id, out } => {
            let root = lake_root_from_config(cli.config.as_deref(), out.clone())?;
            let detail = oddsfox::explore::event_detail(&root, event_id)?;
            println!("{}", serde_json::to_string_pretty(&detail)?);
        }
        Commands::Resolved { out, since } => {
            let root = lake_root_from_config(cli.config.as_deref(), out.clone())?;
            let markets = oddsfox::explore::resolved_markets(&root, since.as_deref())?;
            println!("{}", serde_json::to_string_pretty(&markets)?);
        }
        Commands::Top { by, out, limit } => {
            let root = lake_root_from_config(cli.config.as_deref(), out.clone())?;
            let markets = oddsfox::snapshot::top_markets(&root, TopBy::from(*by), *limit)?;
            println!("{}", serde_json::to_string_pretty(&markets)?);
        }
        Commands::Metrics { target } => match target {
            MetricsCommands::Market { market_id, out } => {
                let root = lake_root_from_config(cli.config.as_deref(), out.clone())?;
                let metrics = oddsfox::metrics::market_metrics(&root, market_id)?;
                println!("{}", serde_json::to_string_pretty(&metrics)?);
            }
        },
        Commands::Check { out } => {
            let root = lake_root_from_config(cli.config.as_deref(), out.clone())?;
            oddsfox::check::run(&root)?;
        }
        Commands::Repair { out } => {
            let root = lake_root_from_config(cli.config.as_deref(), out.clone())?;
            oddsfox::repair::run(&root).await?;
        }
        Commands::Clean { out, dry_run } => {
            let root = lake_root_from_config(cli.config.as_deref(), out.clone())?;
            oddsfox::clean::run(&root, *dry_run)?;
        }
        Commands::Schema { table } => {
            let table: Table = table.parse()?;
            oddsfox::schema::print_schema(table);
        }
        Commands::Contract { out } => {
            let root = lake_root_from_config(cli.config.as_deref(), out.clone())?;
            oddsfox::contract::refresh_contract(&oddsfox::paths::LakePaths::new(&root))?;
            oddsfox::contract::print_contract()?;
        }
        Commands::Duckdb { out, db } => {
            let root = lake_root_from_config(cli.config.as_deref(), out.clone())?;
            oddsfox::duckdb::run(&oddsfox::config::DuckDbOptions {
                out: root.clone(),
                db: db.clone().unwrap_or_else(|| default_db_for_lake(&oddsfox::paths::LakePaths::new(&root))),
            })?;
        }
        Commands::Sql { query, out, db } => {
            let root = lake_root_from_config(cli.config.as_deref(), out.clone())?;
            let db_path = db.clone().unwrap_or_else(|| {
                default_db_for_lake(&oddsfox::paths::LakePaths::new(&root))
            });
            oddsfox::sql_cmd::run_adhoc(&root, &db_path, query)?;
        }
        Commands::Serve { port, out, db } => {
            let root = lake_root_from_config(cli.config.as_deref(), out.clone())?;
            oddsfox::server::serve(oddsfox::config::ServeOptions {
                out: root.clone(),
                port: *port,
                db: db.clone().unwrap_or_else(|| default_db_for_lake(&oddsfox::paths::LakePaths::new(&root))),
            })
            .await?;
        }
        Commands::Stats { out } => {
            let root = lake_root_from_config(cli.config.as_deref(), out.clone())?;
            oddsfox::ops::stats(&root)?;
        }
        Commands::Head {
            out,
            export_dir,
            limit,
        } => {
            let root = lake_root_from_config(cli.config.as_deref(), out.clone())?;
            let export_dir =
                export_dir.clone().unwrap_or_else(|| oddsfox::head::default_export_dir(&root));
            oddsfox::head::run(&oddsfox::head::HeadOptions {
                out: root,
                export_dir,
                limit: *limit,
            })?;
        }
    }

    Ok(())
}
