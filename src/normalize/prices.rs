use std::sync::Arc;

use arrow::array::{
    ArrayRef, Float64Builder, Int32Builder, RecordBatch, StringBuilder, TimestampMillisecondBuilder,
};

use crate::clob::rest::PriceHistoryPoint;
use crate::error::Result;
use crate::schema::prices as prices_schema;

pub fn prices_batch(
    token_id: &str,
    market_id: Option<&str>,
    points: &[PriceHistoryPoint],
    source: &str,
    fidelity_minutes: Option<i32>,
    run_id: &str,
) -> Result<RecordBatch> {
    let schema = prices_schema::schema();
    let mut token_col = StringBuilder::new();
    let mut market_col = StringBuilder::new();
    let mut ts = TimestampMillisecondBuilder::new();
    let mut price = Float64Builder::new();
    let mut fidelity = Int32Builder::new();
    let mut meta = super::IngestMetaBuilders::new();

    for point in points {
        token_col.append_value(token_id);
        market_col.append_option(market_id);
        let millis = if point.t > 1_000_000_000_000 {
            point.t
        } else {
            point.t * 1000
        };
        ts.append_value(millis);
        price.append_value(point.p);
        if let Some(f) = fidelity_minutes {
            fidelity.append_value(f);
        } else {
            fidelity.append_null();
        }
        meta.append(source, None, None, run_id);
    }

    let mut columns: Vec<ArrayRef> = vec![
        Arc::new(token_col.finish()),
        Arc::new(market_col.finish()),
        Arc::new(ts.finish()),
        Arc::new(price.finish()),
        Arc::new(fidelity.finish()),
    ];
    columns.extend(meta.finish());
    Ok(RecordBatch::try_new(schema, columns)?)
}

pub fn point_timestamp_secs(point: &PriceHistoryPoint) -> i64 {
    if point.t > 1_000_000_000_000 {
        point.t / 1000
    } else {
        point.t
    }
}

pub fn merge_price_history(
    existing: Vec<PriceHistoryPoint>,
    incoming: &[PriceHistoryPoint],
    window_start_secs: i64,
    window_end_secs: i64,
) -> Vec<PriceHistoryPoint> {
    use std::collections::BTreeMap;

    let mut merged = BTreeMap::new();
    for point in existing {
        let ts = point_timestamp_secs(&point);
        if ts < window_start_secs || ts > window_end_secs {
            merged.insert(ts, point);
        }
    }
    for point in incoming {
        merged.insert(point_timestamp_secs(point), point.clone());
    }
    merged.into_values().collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clob::rest::PriceHistoryPoint;

    #[test]
    fn prices_batch_has_ten_columns_and_populated_market_id() {
        let points = vec![
            PriceHistoryPoint {
                t: 1_700_000_000,
                p: 0.55,
            },
            PriceHistoryPoint {
                t: 1_700_000_000_000,
                p: 0.60,
            },
        ];
        let batch = prices_batch(
            "token-1",
            Some("market-1"),
            &points,
            "clob_prices_history",
            Some(60),
            "run-1",
        )
        .unwrap();
        assert_eq!(batch.num_columns(), 10);
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.schema().field(1).name(), "market_id");
        assert_eq!(batch.schema().field(4).name(), "fidelity_minutes");
        assert_eq!(batch.schema().field(5).name(), "source");
        assert_eq!(batch.schema().field(6).name(), "raw_url");
        assert_eq!(batch.schema().field(7).name(), "raw_sha256");
        assert_eq!(batch.schema().field(8).name(), "ingested_at");
        assert_eq!(batch.schema().field(9).name(), "run_id");
    }

    #[test]
    fn merge_replaces_points_inside_window() {
        let existing = vec![
            PriceHistoryPoint { t: 100, p: 0.10 },
            PriceHistoryPoint { t: 200, p: 0.20 },
            PriceHistoryPoint { t: 300, p: 0.30 },
        ];
        let incoming = vec![PriceHistoryPoint { t: 200, p: 0.25 }];
        let merged = merge_price_history(existing, &incoming, 150, 250);
        assert_eq!(merged.len(), 3);
        assert_eq!(merged[0].t, 100);
        assert_eq!(merged[1].p, 0.25);
        assert_eq!(merged[2].t, 300);
    }
}
