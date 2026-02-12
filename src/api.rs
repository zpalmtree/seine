use std::fmt;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use reqwest::blocking::{Client, Response};
use reqwest::header::{HeaderMap, HeaderValue, AUTHORIZATION, CONTENT_TYPE};
use reqwest::StatusCode;
use serde::Serialize;
use serde_json::Value;

use crate::types::{BlockTemplateResponse, SubmitBlockResponse};

#[derive(Debug, Serialize)]
struct CompactSubmitPayload<'a> {
    template_id: &'a str,
    nonce: u64,
}

#[derive(Debug, Serialize)]
struct WalletLoadPayload<'a> {
    password: &'a str,
}

#[derive(Debug)]
pub struct ApiStatusError {
    endpoint: String,
    status: StatusCode,
    message: String,
}

impl ApiStatusError {
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    pub fn status(&self) -> StatusCode {
        self.status
    }

    pub fn message(&self) -> &str {
        &self.message
    }
}

impl fmt::Display for ApiStatusError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} failed ({}): {}",
            self.endpoint, self.status, self.message
        )
    }
}

impl std::error::Error for ApiStatusError {}

#[derive(Clone)]
pub struct ApiClient {
    json_client: Client,
    stream_client: Client,
    base_url: String,
}

impl ApiClient {
    pub fn new(base_url: String, token: String, timeout: Duration) -> Result<Self> {
        let mut headers = HeaderMap::new();

        let auth_value = format!("Bearer {token}");
        headers.insert(
            AUTHORIZATION,
            HeaderValue::from_str(&auth_value).context("invalid authorization header")?,
        );
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));

        let json_client = Client::builder()
            .default_headers(headers.clone())
            .timeout(timeout)
            .build()
            .context("failed to build HTTP JSON client")?;

        // Dedicated SSE client without global request timeout.
        let stream_client = Client::builder()
            .default_headers(headers)
            .build()
            .context("failed to build HTTP stream client")?;

        Ok(Self {
            json_client,
            stream_client,
            base_url,
        })
    }

    pub fn get_block_template(&self) -> Result<BlockTemplateResponse> {
        let url = format!("{}/api/mining/blocktemplate", self.base_url);
        let resp = self
            .json_client
            .get(url)
            .send()
            .context("request to blocktemplate endpoint failed")?;

        decode_json_response(resp, "blocktemplate")
    }

    pub fn submit_block<T: Serialize>(
        &self,
        block: &T,
        template_id: Option<&str>,
        nonce: u64,
    ) -> Result<SubmitBlockResponse> {
        let url = format!("{}/api/mining/submitblock", self.base_url);
        let request = self.json_client.post(url);
        let request = if let Some(template_id) = template_id {
            request.json(&CompactSubmitPayload { template_id, nonce })
        } else {
            request.json(block)
        };
        let resp = request
            .send()
            .context("request to submitblock endpoint failed")?;

        decode_json_response(resp, "submitblock")
    }

    pub fn load_wallet(&self, password: &str) -> Result<()> {
        let url = format!("{}/api/wallet/load", self.base_url);
        let payload = WalletLoadPayload { password };
        let resp = self
            .json_client
            .post(url)
            .json(&payload)
            .send()
            .context("request to wallet/load endpoint failed")?;

        let _: Value = decode_json_response(resp, "wallet/load")?;
        Ok(())
    }

    pub fn open_events_stream(&self) -> Result<Response> {
        let url = format!("{}/api/events", self.base_url);
        self.stream_client
            .get(url)
            .send()
            .context("request to events endpoint failed")
    }
}

pub fn is_no_wallet_loaded_error(err: &anyhow::Error) -> bool {
    let Some(api_err) = err.downcast_ref::<ApiStatusError>() else {
        return false;
    };
    api_err.endpoint() == "blocktemplate"
        && api_err.status() == StatusCode::SERVICE_UNAVAILABLE
        && api_err.message().trim() == "no wallet loaded"
}

pub fn is_wallet_already_loaded_error(err: &anyhow::Error) -> bool {
    let Some(api_err) = err.downcast_ref::<ApiStatusError>() else {
        return false;
    };
    api_err.endpoint() == "wallet/load"
        && api_err.status() == StatusCode::CONFLICT
        && api_err.message().trim() == "wallet already loaded"
}

fn decode_json_response<T: serde::de::DeserializeOwned>(
    resp: Response,
    endpoint: &str,
) -> Result<T> {
    if resp.status().is_success() {
        return resp
            .json::<T>()
            .with_context(|| format!("failed to decode {endpoint} response JSON"));
    }

    let status = resp.status();
    let body = resp.text().unwrap_or_default();
    let message = if let Ok(value) = serde_json::from_str::<Value>(&body) {
        value
            .get("error")
            .and_then(Value::as_str)
            .map(str::to_string)
            .unwrap_or(body)
    } else {
        body
    };

    Err(anyhow!(ApiStatusError {
        endpoint: endpoint.to_string(),
        status,
        message,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use httpmock::prelude::*;
    use serde_json::json;

    fn test_client(server: &MockServer) -> ApiClient {
        let base = server.url("").trim_end_matches('/').to_string();
        ApiClient::new(base, "testtoken".to_string(), Duration::from_secs(5))
            .expect("test client should be created")
    }

    #[test]
    fn get_block_template_success() {
        let server = MockServer::start();
        let expected_target =
            "000000000000f424000000000000000000000000000000000000000000000000".to_string();
        let expected_header = "11".repeat(92);

        let mock = server.mock(|when, then| {
            when.method(GET)
                .path("/api/mining/blocktemplate")
                .header("authorization", "Bearer testtoken");
            then.status(200).json_body(json!({
                "block": {"header": {"height": 123, "difficulty": 999, "nonce": 0}},
                "target": expected_target,
                "header_base": expected_header
            }));
        });

        let client = test_client(&server);
        let resp = client
            .get_block_template()
            .expect("block template request should succeed");
        assert_eq!(resp.target.len(), 64);
        assert_eq!(resp.header_base.len(), 184);
        mock.assert();
    }

    #[test]
    fn submit_block_surfaces_json_error() {
        let server = MockServer::start();

        let mock = server.mock(|when, then| {
            when.method(POST)
                .path("/api/mining/submitblock")
                .header("authorization", "Bearer testtoken");
            then.status(400).json_body(json!({"error": "invalid_pow"}));
        });

        let client = test_client(&server);
        let err = client
            .submit_block(&json!({"header": {"nonce": 1}}), None, 1)
            .expect_err("submit should fail");
        assert!(format!("{err:#}").contains("invalid_pow"));
        mock.assert();
    }

    #[test]
    fn submit_block_compact_payload() {
        let server = MockServer::start();

        let mock = server.mock(|when, then| {
            when.method(POST)
                .path("/api/mining/submitblock")
                .header("authorization", "Bearer testtoken")
                .json_body(json!({"template_id": "tmpl-1", "nonce": 7}));
            then.status(200).json_body(json!({
                "accepted": true,
                "hash": "abcd",
                "height": 1
            }));
        });

        let client = test_client(&server);
        let resp = client
            .submit_block(&json!({"header": {"nonce": 999}}), Some("tmpl-1"), 7)
            .expect("compact submit should succeed");
        assert!(resp.accepted);
        mock.assert();
    }

    #[test]
    fn no_wallet_loaded_error_is_classified() {
        let server = MockServer::start();
        let mock = server.mock(|when, then| {
            when.method(GET)
                .path("/api/mining/blocktemplate")
                .header("authorization", "Bearer testtoken");
            then.status(503)
                .json_body(json!({"error": "no wallet loaded"}));
        });

        let client = test_client(&server);
        let err = client
            .get_block_template()
            .expect_err("blocktemplate request should fail");
        assert!(is_no_wallet_loaded_error(&err));
        mock.assert();
    }

    #[test]
    fn wallet_already_loaded_error_is_classified() {
        let server = MockServer::start();
        let mock = server.mock(|when, then| {
            when.method(POST)
                .path("/api/wallet/load")
                .header("authorization", "Bearer testtoken");
            then.status(409)
                .json_body(json!({"error": "wallet already loaded"}));
        });

        let client = test_client(&server);
        let err = client
            .load_wallet("secret")
            .expect_err("wallet load should fail");
        assert!(is_wallet_already_loaded_error(&err));
        mock.assert();
    }

    #[test]
    fn load_wallet_success() {
        let server = MockServer::start();
        let mock = server.mock(|when, then| {
            when.method(POST)
                .path("/api/wallet/load")
                .header("authorization", "Bearer testtoken")
                .json_body(json!({"password": "secret"}));
            then.status(200).json_body(json!({"loaded": true}));
        });

        let client = test_client(&server);
        client
            .load_wallet("secret")
            .expect("wallet load should succeed");
        mock.assert();
    }
}
