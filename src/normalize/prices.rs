use std::sync::Arc;

use arrow::array::{
    ArrayRef, Float64Builder, Int32Builder, RecordBatch, StringBuilder,
    TimestampMillisecondBuilder,
};
use chrono::Utc;

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
    let mut source_col = StringBuilder::new();
    let mut raw_url = StringBuilder::new();
    let mut raw_sha = StringBuilder::new();
    let mut ingested_at = TimestampMillisecondBuilder::new();
    let mut run_id_col = StringBuilder::new();
    let now = Utc::now().timestamp_millis();

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
        source_col.append_value(source);
        raw_url.append_null();
        raw_sha.append_null();
        ingested_at.append_value(now);
        run_id_col.append_value(run_id);
    }

    let columns: Vec<ArrayRef> = vec![
        Arc::new(token_col.finish()),
        Arc::new(market_col.finish()),
        Arc::new(ts.finish()),
        Arc::new(price.finish()),
        Arc::new(fidelity.finish()),
        Arc::new(source_col.finish()),
        Arc::new(raw_url.finish()),
        Arc::new(raw_sha.finish()),
        Arc::new(ingested_at.finish()),
        Arc::new(run_id_col.finish()),
    ];
    Ok(RecordBatch::try_new(schema, columns)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clob::rest::PriceHistoryPoint;

    #[test]
    fn prices_batch_has_ten_columns_and_populated_market_id() {
        let points = vec![
            PriceHistoryPoint { t: 1_700_000_000, p: 0.55 },
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
    }
}
