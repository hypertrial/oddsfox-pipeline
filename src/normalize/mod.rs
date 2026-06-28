mod events;
mod markets;
mod orderbooks;
mod outcomes;
mod prices;
mod resolutions;
mod trades;

pub use events::events_batch;
pub use markets::markets_batch;
pub use orderbooks::{book_levels_batch, new_snapshot_id, orderbooks_batch, SnapshotRecord};
pub use outcomes::outcomes_batch;
pub use prices::{merge_price_history, point_timestamp_secs, prices_batch};
pub use resolutions::resolutions_batch;
pub use trades::trades_batch;

use std::sync::Arc;

use arrow::array::{ArrayRef, StringBuilder, TimestampMillisecondBuilder};
use chrono::{DateTime, Utc};

pub fn parse_ts(raw: Option<&str>) -> Option<DateTime<Utc>> {
    raw.and_then(|value| DateTime::parse_from_rfc3339(value).ok())
        .map(|dt| dt.with_timezone(&Utc))
        .or_else(|| {
            raw.and_then(|value| {
                value
                    .parse::<i64>()
                    .ok()
                    .and_then(|secs| DateTime::from_timestamp(secs, 0))
            })
        })
}

pub fn parse_f64(raw: Option<&str>) -> Option<f64> {
    raw.and_then(|value| value.parse::<f64>().ok())
}

pub(crate) struct IngestMetaBuilders {
    source: StringBuilder,
    raw_url: StringBuilder,
    raw_sha: StringBuilder,
    ingested_at: TimestampMillisecondBuilder,
    run_id: StringBuilder,
    now: i64,
}

impl IngestMetaBuilders {
    pub(crate) fn new() -> Self {
        Self::new_at(Utc::now().timestamp_millis())
    }

    pub(crate) fn new_at(now: i64) -> Self {
        Self {
            source: StringBuilder::new(),
            raw_url: StringBuilder::new(),
            raw_sha: StringBuilder::new(),
            ingested_at: TimestampMillisecondBuilder::new(),
            run_id: StringBuilder::new(),
            now,
        }
    }

    pub(crate) fn append(
        &mut self,
        source: &str,
        raw_url: Option<&str>,
        raw_sha: Option<&str>,
        run_id: &str,
    ) {
        self.source.append_value(source);
        self.raw_url
            .append_option(raw_url.filter(|value| !value.is_empty()));
        self.raw_sha
            .append_option(raw_sha.filter(|value| !value.is_empty()));
        self.ingested_at.append_value(self.now);
        self.run_id.append_value(run_id);
    }

    pub(crate) fn finish(&mut self) -> Vec<ArrayRef> {
        vec![
            Arc::new(self.source.finish()),
            Arc::new(self.raw_url.finish()),
            Arc::new(self.raw_sha.finish()),
            Arc::new(self.ingested_at.finish()),
            Arc::new(self.run_id.finish()),
        ]
    }
}
