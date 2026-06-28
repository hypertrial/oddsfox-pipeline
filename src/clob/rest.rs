use serde::Deserialize;

use crate::error::Result;
use crate::http::HttpClient;

#[derive(Debug, Clone, Deserialize, serde::Serialize)]
pub struct OrderBookResponse {
    pub hash: Option<String>,
    pub market: Option<String>,
    pub asset_id: Option<String>,
    pub timestamp: Option<String>,
    pub bids: Option<Vec<BookLevelJson>>,
    pub asks: Option<Vec<BookLevelJson>>,
    pub min_order_size: Option<String>,
    pub tick_size: Option<String>,
    pub neg_risk: Option<bool>,
}

#[derive(Debug, Clone, Deserialize, serde::Serialize)]
pub struct BookLevelJson {
    pub price: String,
    pub size: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PriceHistoryPoint {
    pub t: i64,
    pub p: f64,
}

#[derive(Clone)]
pub struct ClobClient {
    pub base_url: String,
    pub http: HttpClient,
}

impl ClobClient {
    pub fn new(base_url: impl Into<String>, http: HttpClient) -> Self {
        Self {
            base_url: base_url.into(),
            http,
        }
    }

    pub async fn get_book(&self, token_id: &str) -> Result<OrderBookResponse> {
        let url = format!("{}/book?token_id={token_id}", self.base_url);
        let body = self.http.get_bytes(&url).await?;
        Ok(serde_json::from_slice(&body)?)
    }

    pub async fn get_prices_history(
        &self,
        token_id: &str,
        interval: Option<&str>,
        fidelity: Option<u32>,
        start_ts: Option<i64>,
        end_ts: Option<i64>,
    ) -> Result<Vec<PriceHistoryPoint>> {
        let mut url = format!("{}/prices-history?market={token_id}", self.base_url);
        if let Some(interval) = interval {
            url.push_str(&format!("&interval={interval}"));
        }
        if let Some(fidelity) = fidelity {
            url.push_str(&format!("&fidelity={fidelity}"));
        }
        if let Some(start_ts) = start_ts {
            url.push_str(&format!("&startTs={start_ts}"));
        }
        if let Some(end_ts) = end_ts {
            url.push_str(&format!("&endTs={end_ts}"));
        }
        let json = self.http.get_json(&url).await?;
        let history = json
            .get("history")
            .and_then(|v| serde_json::from_value(v.clone()).ok())
            .unwrap_or_default();
        Ok(history)
    }
}
