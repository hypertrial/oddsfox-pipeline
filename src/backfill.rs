use crate::config::{
    BackfillOptions, BackfillSource, DuckDbOptions, Source, SyncMarketsOptions, SyncPricesOptions,
    DEFAULT_BACKFILL_INTERVAL,
};
use crate::error::Result;

pub async fn run(options: BackfillOptions) -> Result<()> {
    crate::init::run(&options.out)?;

    match options.source {
        BackfillSource::Polymarket => backfill_polymarket(&options).await?,
        BackfillSource::Kalshi => backfill_kalshi(&options).await?,
        BackfillSource::All => {
            backfill_polymarket(&options).await?;
            backfill_kalshi(&options).await?;
        }
    }

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

    println!("backfill complete: catalog at `{}`", db_path.display());
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

async fn backfill_polymarket(options: &BackfillOptions) -> Result<()> {
    let sync_summary = crate::sync::sync_markets(SyncMarketsOptions {
        out: options.out.clone(),
        source: Source::Polymarket,
        active: options.active || (!options.closed && !options.all),
        closed: options.closed,
        all: options.all,
        status: None,
        series: None,
        event: None,
        tag: options.tag.clone(),
        since: None,
        limit: options.limit,
        resume: true,
        overwrite: false,
        gamma_base_url: options.gamma_base_url.clone(),
        kalshi_rest_base_url: options.kalshi_rest_base_url.clone(),
        kalshi_key_id: options.kalshi_key_id.clone(),
        kalshi_private_key_path: options.kalshi_private_key_path.clone(),
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
        source: Source::Polymarket,
        market_id: None,
        series: None,
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
        period: None,
        since: options.since,
        until: options.until,
        overwrite: options.overwrite,
        concurrency: options.concurrency,
        clob_base_url: options.clob_base_url.clone(),
        kalshi_rest_base_url: options.kalshi_rest_base_url.clone(),
        kalshi_key_id: options.kalshi_key_id.clone(),
        kalshi_private_key_path: options.kalshi_private_key_path.clone(),
        requests_per_second: options.requests_per_second,
        max_retries: options.max_retries,
        user_agent: options.user_agent.clone(),
    })
    .await?;

    println!(
        "polymarket backfill complete: {} events, {} markets",
        sync_summary.events,
        sync_summary.markets
    );
    Ok(())
}

async fn backfill_kalshi(options: &BackfillOptions) -> Result<()> {
    let markets_options = SyncMarketsOptions {
        out: options.out.clone(),
        source: Source::Kalshi,
        active: true,
        closed: false,
        all: options.all,
        status: None,
        series: options.tag.clone(),
        event: None,
        tag: None,
        since: None,
        limit: options.limit,
        resume: true,
        overwrite: false,
        gamma_base_url: options.gamma_base_url.clone(),
        kalshi_rest_base_url: options.kalshi_rest_base_url.clone(),
        kalshi_key_id: options.kalshi_key_id.clone(),
        kalshi_private_key_path: options.kalshi_private_key_path.clone(),
        requests_per_second: options.requests_per_second,
        max_retries: options.max_retries,
        user_agent: options.user_agent.clone(),
        raw_retention_days: options.raw_retention_days,
    };
    if let Some(cutoff) = crate::kalshi::historical_cutoff(&markets_options).await? {
        println!("kalshi historical cutoff: {cutoff}");
    }
    let sync_summary = crate::kalshi::sync_markets(markets_options).await?;
    for (market_id, series) in kalshi_markets_for_prices(options)? {
        crate::kalshi::sync_prices(SyncPricesOptions {
            out: options.out.clone(),
            source: Source::Kalshi,
            market_id: Some(market_id),
            series,
            active: false,
            all: false,
            filter_active: None,
            tag: None,
            limit: None,
            top_limit: None,
            interval: None,
            fidelity: options.fidelity,
            period: options.fidelity,
            since: options.since,
            until: options.until,
            overwrite: options.overwrite,
            concurrency: options.concurrency,
            clob_base_url: options.clob_base_url.clone(),
            kalshi_rest_base_url: options.kalshi_rest_base_url.clone(),
            kalshi_key_id: options.kalshi_key_id.clone(),
            kalshi_private_key_path: options.kalshi_private_key_path.clone(),
            requests_per_second: options.requests_per_second,
            max_retries: options.max_retries,
            user_agent: options.user_agent.clone(),
        })
        .await?;
    }
    println!(
        "kalshi backfill complete: {} events, {} markets",
        sync_summary.events, sync_summary.markets
    );
    Ok(())
}

fn kalshi_markets_for_prices(options: &BackfillOptions) -> Result<Vec<(String, Option<String>)>> {
    let paths = crate::paths::LakePaths::new(&options.out);
    let glob = paths.duckdb_parquet_glob(crate::config::Table::Markets);
    if !crate::duckdb_engine::glob_exists(&glob) {
        return Ok(Vec::new());
    }
    let conn = crate::duckdb_engine::open_connection(None)?;
    let limit = options.limit.map(|n| format!(" LIMIT {n}")).unwrap_or_default();
    let sql = format!(
        "SELECT market_id, json_extract_string(raw_json, '$.series_ticker')
         FROM read_parquet('{glob}')
         WHERE source = 'kalshi' AND active = true
         ORDER BY market_id{limit}"
    );
    let mut stmt = crate::duckdb_engine::map_duckdb(conn.prepare(&sql))?;
    let rows = crate::duckdb_engine::map_duckdb(stmt.query_map([], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?))
    }))?;
    Ok(rows.filter_map(|row| row.ok()).collect())
}
