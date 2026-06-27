use crate::config::{
    BackfillOptions, DuckDbOptions, SyncMarketsOptions, SyncPricesOptions,
    DEFAULT_BACKFILL_INTERVAL,
};
use crate::error::Result;

pub async fn run(options: BackfillOptions) -> Result<()> {
    crate::init::run(&options.out)?;

    let sync_summary = crate::sync::sync_markets(SyncMarketsOptions {
        out: options.out.clone(),
        active: options.active || (!options.closed && !options.all),
        closed: options.closed,
        all: options.all,
        tag: options.tag.clone(),
        since: None,
        limit: options.limit,
        resume: true,
        overwrite: false,
        gamma_base_url: options.gamma_base_url.clone(),
        requests_per_second: options.requests_per_second,
        max_retries: options.max_retries,
        user_agent: options.user_agent.clone(),
        raw_retention_days: options.raw_retention_days,
    })
    .await?;

    let filter_active = if options.all && !options.active && !options.closed {
        None
    } else if options.active {
        Some(true)
    } else if options.closed {
        Some(false)
    } else {
        None
    };

    crate::prices::sync_prices(SyncPricesOptions {
        out: options.out.clone(),
        market_id: None,
        active: false,
        all: true,
        filter_active,
        tag: options.tag.clone(),
        limit: options.limit,
        top_limit: None,
        interval: options
            .interval
            .clone()
            .or_else(|| Some(DEFAULT_BACKFILL_INTERVAL.to_string())),
        fidelity: options.fidelity,
        since: options.since,
        until: options.until,
        overwrite: options.overwrite,
        concurrency: options.concurrency,
        clob_base_url: options.clob_base_url.clone(),
        requests_per_second: options.requests_per_second,
        max_retries: options.max_retries,
        user_agent: options.user_agent.clone(),
    })
    .await?;

    let db_path = options.db.clone();
    crate::duckdb::run(&DuckDbOptions {
        out: options.out.clone(),
        db: db_path.clone(),
    })?;

    let report = crate::check::check_lake(&options.out)?;
    if !report.issues.is_empty() {
        for issue in report.issues {
            println!("backfill warning: {issue}");
        }
    }

    println!(
        "backfill complete: {} events, {} markets, catalog at `{}`",
        sync_summary.events,
        sync_summary.markets,
        db_path.display()
    );
    println!(
        "query with `oddsfox sql \"SELECT * FROM bronze_prices LIMIT 5\" --out {} --db {}`",
        options.out.display(),
        db_path.display()
    );
    println!(
        "or run `oddsfox serve --out {} --db {} --port {}`",
        options.out.display(),
        db_path.display(),
        options.port
    );
    Ok(())
}
