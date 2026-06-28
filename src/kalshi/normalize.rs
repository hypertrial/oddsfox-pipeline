use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::{
    ArrayRef, BooleanBuilder, Float64Builder, Int32Builder, RecordBatch, StringBuilder,
    TimestampMillisecondBuilder,
};
use chrono::{DateTime, Utc};

use crate::clob::book::{parse_book, ParsedBook};
use crate::clob::rest::{BookLevelJson, OrderBookResponse, PriceHistoryPoint};
use crate::error::Result;
use crate::normalize::new_snapshot_id;
use crate::normalize::SnapshotRecord;
use crate::schema;

use super::models::{
    KalshiCandlestick, KalshiEvent, KalshiMarket, KalshiOrderbookEnvelope, KalshiTrade,
};

const SOURCE: &str = "kalshi";

pub fn kalshi_market_id(ticker: &str) -> String {
    format!("kalshi:{}", strip_kalshi_market_id(ticker))
}

pub fn kalshi_event_id(ticker: &str) -> String {
    format!("kalshi:{}", strip_kalshi_market_id(ticker))
}

pub fn kalshi_token_id(ticker: &str, side: &str) -> String {
    format!("kalshi:{}:{}", strip_kalshi_market_id(ticker), side.to_ascii_lowercase())
}

pub fn strip_kalshi_market_id(id: &str) -> &str {
    id.strip_prefix("kalshi:").unwrap_or(id)
}

pub fn events_from_markets(markets: &[KalshiMarket]) -> Vec<KalshiEvent> {
    let mut events = BTreeMap::new();
    for market in markets {
        let Some(event_ticker) = market.event_ticker.as_ref() else {
            continue;
        };
        events.entry(event_ticker.clone()).or_insert_with(|| KalshiEvent {
            event_ticker: Some(event_ticker.clone()),
            series_ticker: market.series_ticker.clone(),
            ticker: Some(event_ticker.clone()),
            title: market.title.clone(),
            sub_title: market.subtitle.clone().or_else(|| market.sub_title.clone()),
            category: market.series_ticker.clone(),
            tags: market.series_ticker.iter().cloned().collect(),
        });
    }
    events.into_values().collect()
}

pub fn events_batch(events: &[KalshiEvent], raw_url: &str, raw_sha256: &str, run_id: &str) -> Result<RecordBatch> {
    let schema = schema::events::schema();
    let mut event_id = StringBuilder::new();
    let mut slug = StringBuilder::new();
    let mut title = StringBuilder::new();
    let mut description = StringBuilder::new();
    let mut category = StringBuilder::new();
    let mut tags = StringBuilder::new();
    let mut active = BooleanBuilder::new();
    let mut closed = BooleanBuilder::new();
    let mut start_time = TimestampMillisecondBuilder::new();
    let mut end_time = TimestampMillisecondBuilder::new();
    let mut created_at = TimestampMillisecondBuilder::new();
    let mut updated_at = TimestampMillisecondBuilder::new();
    let mut raw_json = StringBuilder::new();
    let mut meta = MetaBuilders::new();
    for event in events {
        let ticker = event.event_ticker.as_deref().or(event.ticker.as_deref()).unwrap_or("unknown");
        event_id.append_value(kalshi_event_id(ticker));
        slug.append_option(event.event_ticker.as_deref());
        title.append_option(event.title.as_deref());
        description.append_option(event.sub_title.as_deref());
        category.append_option(event.category.as_deref().or(event.series_ticker.as_deref()));
        tags.append_value(serde_json::to_string(&event.tags).unwrap_or_else(|_| "[]".into()));
        active.append_null();
        closed.append_null();
        start_time.append_null();
        end_time.append_null();
        created_at.append_null();
        updated_at.append_null();
        raw_json.append_value(serde_json::to_string(event).unwrap_or_else(|_| "{}".into()));
        meta.append(raw_url, raw_sha256, run_id);
    }
    Ok(RecordBatch::try_new(schema, vec![
        Arc::new(event_id.finish()),
        Arc::new(slug.finish()),
        Arc::new(title.finish()),
        Arc::new(description.finish()),
        Arc::new(category.finish()),
        Arc::new(tags.finish()),
        Arc::new(active.finish()),
        Arc::new(closed.finish()),
        Arc::new(start_time.finish()),
        Arc::new(end_time.finish()),
        Arc::new(created_at.finish()),
        Arc::new(updated_at.finish()),
        Arc::new(raw_json.finish()),
        meta.source(),
        meta.raw_url(),
        meta.raw_sha(),
        meta.ingested_at(),
        meta.run_id(),
    ])?)
}

pub fn markets_batch(markets: &[KalshiMarket], raw_url: &str, raw_sha256: &str, run_id: &str) -> Result<RecordBatch> {
    let schema = schema::markets::schema();
    let mut market_id = StringBuilder::new();
    let mut event_id = StringBuilder::new();
    let mut condition_id = StringBuilder::new();
    let mut question_id = StringBuilder::new();
    let mut slug = StringBuilder::new();
    let mut question = StringBuilder::new();
    let mut description = StringBuilder::new();
    let mut active = BooleanBuilder::new();
    let mut closed = BooleanBuilder::new();
    let mut resolved = BooleanBuilder::new();
    let mut enable_order_book = BooleanBuilder::new();
    let mut neg_risk = BooleanBuilder::new();
    let mut liquidity = Float64Builder::new();
    let mut volume = Float64Builder::new();
    let mut volume_24h = Float64Builder::new();
    let mut open_interest = Float64Builder::new();
    let mut close_time = TimestampMillisecondBuilder::new();
    let mut resolution_time = TimestampMillisecondBuilder::new();
    let mut resolution_source = StringBuilder::new();
    let mut raw_json = StringBuilder::new();
    let mut meta = MetaBuilders::new();
    for market in markets {
        market_id.append_value(kalshi_market_id(&market.ticker));
        event_id.append_option(market.event_ticker.as_deref().map(kalshi_event_id));
        condition_id.append_null();
        question_id.append_option(Some(&market.ticker));
        slug.append_option(Some(&market.ticker));
        question.append_option(market.title.as_deref());
        description.append_option(market.subtitle.as_deref().or(market.sub_title.as_deref()));
        active.append_value(matches!(market.status.as_deref(), Some("open" | "paused" | "unopened")));
        closed.append_value(matches!(market.status.as_deref(), Some("closed" | "settled")));
        resolved.append_value(matches!(market.status.as_deref(), Some("settled")) || market.result.is_some());
        enable_order_book.append_value(true);
        neg_risk.append_null();
        append_f64(&mut liquidity, market.liquidity);
        append_f64(&mut volume, market.volume);
        append_f64(&mut volume_24h, market.volume_24h);
        append_f64(&mut open_interest, market.open_interest);
        append_ts(&mut close_time, parse_time(market.close_time.as_deref()));
        append_ts(&mut resolution_time, settlement_time(market));
        resolution_source.append_option(market.result.as_deref().map(|_| "kalshi"));
        raw_json.append_value(serde_json::to_string(market).unwrap_or_else(|_| "{}".into()));
        meta.append(raw_url, raw_sha256, run_id);
    }
    Ok(RecordBatch::try_new(schema, vec![
        Arc::new(market_id.finish()), Arc::new(event_id.finish()), Arc::new(condition_id.finish()),
        Arc::new(question_id.finish()), Arc::new(slug.finish()), Arc::new(question.finish()),
        Arc::new(description.finish()), Arc::new(active.finish()), Arc::new(closed.finish()),
        Arc::new(resolved.finish()), Arc::new(enable_order_book.finish()), Arc::new(neg_risk.finish()),
        Arc::new(liquidity.finish()), Arc::new(volume.finish()), Arc::new(volume_24h.finish()),
        Arc::new(open_interest.finish()), Arc::new(close_time.finish()), Arc::new(resolution_time.finish()),
        Arc::new(resolution_source.finish()), Arc::new(raw_json.finish()), meta.source(), meta.raw_url(),
        meta.raw_sha(), meta.ingested_at(), meta.run_id(),
    ])?)
}

pub fn outcomes_batch(markets: &[KalshiMarket], raw_url: &str, raw_sha256: &str, run_id: &str) -> Result<RecordBatch> {
    let schema = schema::outcomes::schema();
    let mut market_id = StringBuilder::new();
    let mut outcome_index = Int32Builder::new();
    let mut outcome_name = StringBuilder::new();
    let mut token_id = StringBuilder::new();
    let mut is_winner = BooleanBuilder::new();
    let mut meta = MetaBuilders::new();
    for market in markets {
        for (idx, side) in ["yes", "no"].iter().enumerate() {
            market_id.append_value(kalshi_market_id(&market.ticker));
            outcome_index.append_value(idx as i32);
            outcome_name.append_value(side.to_ascii_uppercase());
            token_id.append_value(kalshi_token_id(&market.ticker, side));
            is_winner.append_value(market.result.as_deref().is_some_and(|r| r.eq_ignore_ascii_case(side)));
            meta.append(raw_url, raw_sha256, run_id);
        }
    }
    Ok(RecordBatch::try_new(schema, vec![
        Arc::new(market_id.finish()), Arc::new(outcome_index.finish()), Arc::new(outcome_name.finish()),
        Arc::new(token_id.finish()), Arc::new(is_winner.finish()), meta.source(), meta.raw_url(),
        meta.raw_sha(), meta.ingested_at(), meta.run_id(),
    ])?)
}

pub fn resolutions_batch(markets: &[KalshiMarket], raw_url: &str, raw_sha256: &str, run_id: &str) -> Result<RecordBatch> {
    let schema = schema::resolutions::schema();
    let mut market_id = StringBuilder::new();
    let mut resolved_at = TimestampMillisecondBuilder::new();
    let mut winning_token_id = StringBuilder::new();
    let mut winning_outcome = StringBuilder::new();
    let mut resolution_source = StringBuilder::new();
    let mut resolution_status = StringBuilder::new();
    let mut raw_json = StringBuilder::new();
    let mut meta = MetaBuilders::new();
    for market in markets.iter().filter(|m| m.result.is_some() || settlement_time(m).is_some()) {
        market_id.append_value(kalshi_market_id(&market.ticker));
        append_ts(&mut resolved_at, settlement_time(market));
        let winner = market.result.as_deref().unwrap_or("unknown").to_ascii_lowercase();
        winning_token_id.append_option(if winner == "yes" || winner == "no" {
            Some(kalshi_token_id(&market.ticker, &winner))
        } else {
            None
        });
        winning_outcome.append_value(winner);
        resolution_source.append_value("kalshi");
        resolution_status.append_value("resolved");
        raw_json.append_value(serde_json::to_string(market).unwrap_or_else(|_| "{}".into()));
        meta.append(raw_url, raw_sha256, run_id);
    }
    Ok(RecordBatch::try_new(schema, vec![
        Arc::new(market_id.finish()), Arc::new(resolved_at.finish()), Arc::new(winning_token_id.finish()),
        Arc::new(winning_outcome.finish()), Arc::new(resolution_source.finish()), Arc::new(resolution_status.finish()),
        Arc::new(raw_json.finish()), meta.source(), meta.raw_url(), meta.raw_sha(), meta.ingested_at(), meta.run_id(),
    ])?)
}

pub fn price_points_from_candlesticks(candles: &[KalshiCandlestick]) -> (Vec<PriceHistoryPoint>, Vec<PriceHistoryPoint>, usize) {
    let mut yes = Vec::new();
    let mut no = Vec::new();
    let mut skipped = 0;
    for candle in candles {
        let Some(ts) = candle.end_period_ts.or(candle.start_period_ts) else {
            skipped += 1;
            continue;
        };
        let price = candle
            .price
            .as_ref()
            .and_then(price_close)
            .or_else(|| midpoint(candle.yes_bid.as_ref(), candle.yes_ask.as_ref()));
        let Some(p) = price else {
            skipped += 1;
            continue;
        };
        yes.push(PriceHistoryPoint { t: ts, p });
        no.push(PriceHistoryPoint { t: ts, p: 1.0 - p });
    }
    (yes, no, skipped)
}

pub fn trades_batch(trades: &[KalshiTrade], run_id: &str) -> Result<RecordBatch> {
    let schema = schema::trades::schema();
    let mut trade_id = StringBuilder::new();
    let mut market_id = StringBuilder::new();
    let mut token_id = StringBuilder::new();
    let mut ts = TimestampMillisecondBuilder::new();
    let mut price = Float64Builder::new();
    let mut size = Float64Builder::new();
    let mut side = StringBuilder::new();
    let mut tx_hash = StringBuilder::new();
    let mut maker = StringBuilder::new();
    let mut taker = StringBuilder::new();
    let mut raw_json = StringBuilder::new();
    let mut meta = MetaBuilders::new();
    for trade in trades {
        let ticker = trade.ticker.as_deref().or(trade.market_ticker.as_deref()).unwrap_or("unknown");
        let side_value = trade.taker_side.as_deref().unwrap_or("yes").to_ascii_lowercase();
        trade_id.append_option(trade.trade_id.as_deref());
        market_id.append_value(kalshi_market_id(ticker));
        token_id.append_value(kalshi_token_id(ticker, &side_value));
        let millis = trade.created_ts.map(|ts| ts * 1000).or_else(|| parse_time(trade.created_time.as_deref()).map(|dt| dt.timestamp_millis()));
        ts.append_value(millis.unwrap_or_else(|| Utc::now().timestamp_millis()));
        append_f64(&mut price, normalize_price(trade.yes_price_dollars.or(trade.yes_price)));
        append_f64(&mut size, trade.count_fp.or(trade.count));
        side.append_value(side_value);
        tx_hash.append_null();
        maker.append_null();
        taker.append_null();
        raw_json.append_value(serde_json::to_string(trade).unwrap_or_else(|_| "{}".into()));
        meta.append("", "", run_id);
    }
    Ok(RecordBatch::try_new(schema, vec![
        Arc::new(trade_id.finish()), Arc::new(market_id.finish()), Arc::new(token_id.finish()),
        Arc::new(ts.finish()), Arc::new(price.finish()), Arc::new(size.finish()), Arc::new(side.finish()),
        Arc::new(tx_hash.finish()), Arc::new(maker.finish()), Arc::new(taker.finish()), Arc::new(raw_json.finish()),
        meta.source(), meta.raw_url(), meta.raw_sha(), meta.ingested_at(), meta.run_id(),
    ])?)
}

pub fn snapshot_records_from_orderbook(ticker: &str, envelope: &KalshiOrderbookEnvelope) -> Vec<SnapshotRecord> {
    let yes_bids = levels(&envelope.orderbook, "yes");
    let no_bids = levels(&envelope.orderbook, "no");
    let yes_book = book_from_bids(ticker, "yes", &yes_bids, &no_bids);
    let no_book = book_from_bids(ticker, "no", &no_bids, &yes_bids);
    [yes_book, no_book]
        .into_iter()
        .map(|book| {
            let parsed: ParsedBook = parse_book(&book);
            SnapshotRecord {
                snapshot_id: new_snapshot_id(),
                token_id: book.asset_id.clone().unwrap_or_default(),
                market_id: Some(kalshi_market_id(ticker)),
                book,
                parsed,
            }
        })
        .collect()
}

fn book_from_bids(ticker: &str, side: &str, bids: &[(f64, f64)], opposite_bids: &[(f64, f64)]) -> OrderBookResponse {
    let asks = opposite_bids
        .iter()
        .map(|(p, size)| BookLevelJson {
            price: format!("{:.4}", 1.0 - p),
            size: size.to_string(),
        })
        .collect::<Vec<_>>();
    OrderBookResponse {
        hash: None,
        market: Some(kalshi_market_id(ticker)),
        asset_id: Some(kalshi_token_id(ticker, side)),
        timestamp: None,
        bids: Some(to_book_levels(bids)),
        asks: Some(asks),
        min_order_size: None,
        tick_size: None,
        neg_risk: None,
    }
}

fn to_book_levels(levels: &[(f64, f64)]) -> Vec<BookLevelJson> {
    levels
        .iter()
        .map(|(price, size)| BookLevelJson {
            price: price.to_string(),
            size: size.to_string(),
        })
        .collect()
}

fn levels(value: &serde_json::Value, key: &str) -> Vec<(f64, f64)> {
    value
        .get(key)
        .and_then(|v| v.as_array())
        .into_iter()
        .flatten()
        .filter_map(|row| {
            let array = row.as_array()?;
            let price = json_f64(array.first()?)?;
            let size = json_f64(array.get(1)?)?;
            Some((normalize_price(Some(price))?, size))
        })
        .collect()
}

fn price_close(price: &super::models::KalshiCandlePrice) -> Option<f64> {
    normalize_price(price.close_dollars.or(price.close))
}

fn midpoint(bid: Option<&super::models::KalshiCandlePrice>, ask: Option<&super::models::KalshiCandlePrice>) -> Option<f64> {
    Some((price_close(bid?)? + price_close(ask?)?) / 2.0)
}

fn normalize_price(value: Option<f64>) -> Option<f64> {
    value.map(|p| if p > 1.0 { p / 100.0 } else { p })
}

fn json_f64(value: &serde_json::Value) -> Option<f64> {
    value.as_f64().or_else(|| value.as_str()?.parse().ok())
}

fn settlement_time(market: &KalshiMarket) -> Option<DateTime<Utc>> {
    market
        .settlement_ts
        .and_then(|ts| DateTime::from_timestamp(ts, 0))
        .or_else(|| parse_time(market.settlement_time.as_deref()))
}

fn parse_time(raw: Option<&str>) -> Option<DateTime<Utc>> {
    crate::normalize::parse_ts(raw)
}

fn append_f64(builder: &mut Float64Builder, value: Option<f64>) {
    if let Some(value) = value {
        builder.append_value(value);
    } else {
        builder.append_null();
    }
}

fn append_ts(builder: &mut TimestampMillisecondBuilder, value: Option<DateTime<Utc>>) {
    if let Some(value) = value {
        builder.append_value(value.timestamp_millis());
    } else {
        builder.append_null();
    }
}

struct MetaBuilders {
    source: StringBuilder,
    raw_url: StringBuilder,
    raw_sha: StringBuilder,
    ingested_at: TimestampMillisecondBuilder,
    run_id: StringBuilder,
    now: i64,
}

impl MetaBuilders {
    fn new() -> Self {
        Self {
            source: StringBuilder::new(),
            raw_url: StringBuilder::new(),
            raw_sha: StringBuilder::new(),
            ingested_at: TimestampMillisecondBuilder::new(),
            run_id: StringBuilder::new(),
            now: Utc::now().timestamp_millis(),
        }
    }

    fn append(&mut self, raw_url: &str, raw_sha: &str, run_id: &str) {
        self.source.append_value(SOURCE);
        if raw_url.is_empty() {
            self.raw_url.append_null();
        } else {
            self.raw_url.append_value(raw_url);
        }
        if raw_sha.is_empty() {
            self.raw_sha.append_null();
        } else {
            self.raw_sha.append_value(raw_sha);
        }
        self.ingested_at.append_value(self.now);
        self.run_id.append_value(run_id);
    }

    fn source(&mut self) -> ArrayRef {
        Arc::new(self.source.finish())
    }

    fn raw_url(&mut self) -> ArrayRef {
        Arc::new(self.raw_url.finish())
    }

    fn raw_sha(&mut self) -> ArrayRef {
        Arc::new(self.raw_sha.finish())
    }

    fn ingested_at(&mut self) -> ArrayRef {
        Arc::new(self.ingested_at.finish())
    }

    fn run_id(&mut self) -> ArrayRef {
        Arc::new(self.run_id.finish())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kalshi_ids_are_prefixed() {
        assert_eq!(kalshi_market_id("KXTEST-26"), "kalshi:KXTEST-26");
        assert_eq!(kalshi_token_id("KXTEST-26", "YES"), "kalshi:KXTEST-26:yes");
    }

    #[test]
    fn candlesticks_make_yes_and_no_prices() {
        let candles = vec![KalshiCandlestick {
            end_period_ts: Some(100),
            price: Some(super::super::models::KalshiCandlePrice {
                close: None,
                close_dollars: Some(0.62),
            }),
            ..Default::default()
        }];
        let (yes, no, skipped) = price_points_from_candlesticks(&candles);
        assert_eq!(skipped, 0);
        assert_eq!(yes[0].p, 0.62);
        assert!((no[0].p - 0.38).abs() < 1e-9);
    }

    #[test]
    fn orderbook_complements_opposite_bids_into_asks() {
        let book = KalshiOrderbookEnvelope {
            orderbook: serde_json::json!({
                "yes": [[48, 10]],
                "no": [[47, 20]]
            }),
        };
        let records = snapshot_records_from_orderbook("KXTEST-26", &book);
        assert_eq!(records.len(), 2);
        assert!((records[0].parsed.best_bid.unwrap() - 0.48).abs() < 1e-9);
        assert!((records[0].parsed.best_ask.unwrap() - 0.53).abs() < 1e-9);
    }
}
