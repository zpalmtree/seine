use std::path::Path;

use crate::config::read_token_from_cookie_file;
use crate::daemon_api::ApiClient;

#[derive(Debug, Clone, Eq, PartialEq)]
pub(super) enum TokenRefreshOutcome {
    Refreshed,
    Unchanged,
    Unavailable,
    Failed(String),
}

pub(super) fn refresh_api_token_from_cookie(
    client: &ApiClient,
    cookie_path: Option<&Path>,
) -> TokenRefreshOutcome {
    let Some(cookie_path) = cookie_path else {
        return TokenRefreshOutcome::Unavailable;
    };

    let token = match read_token_from_cookie_file(cookie_path) {
        Ok(token) => token,
        Err(err) => {
            return TokenRefreshOutcome::Failed(format!(
                "failed reading API cookie {}: {err:#}",
                cookie_path.display()
            ))
        }
    };

    match client.replace_token(token) {
        Ok(true) => TokenRefreshOutcome::Refreshed,
        Ok(false) => TokenRefreshOutcome::Unchanged,
        Err(err) => TokenRefreshOutcome::Failed(format!("failed updating API token: {err:#}")),
    }
}
