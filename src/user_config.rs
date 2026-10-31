use std::fs;
use std::path::Path;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

use crate::config::MiningMode;

pub const USER_CONFIG_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct UserConfig {
    pub schema_version: u32,
    pub mode: Option<MiningMode>,
    pub address: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub payout_memo: Option<String>,
    pub pool_url: Option<String>,
    pub pool_worker: Option<String>,
    pub dev_fee_pool_worker: Option<String>,
}

pub fn read_user_config(path: &Path) -> Result<Option<UserConfig>> {
    if !path.exists() {
        return Ok(None);
    }
    let raw = fs::read_to_string(path)
        .with_context(|| format!("failed to read user config at {}", path.display()))?;
    let mut cfg: UserConfig = serde_json::from_str(&raw)
        .with_context(|| format!("failed to parse user config at {}", path.display()))?;
    if cfg.schema_version == 0 {
        cfg.schema_version = USER_CONFIG_SCHEMA_VERSION;
    }
    Ok(Some(cfg))
}

pub fn write_user_config(path: &Path, cfg: &UserConfig) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create config directory {}", parent.display()))?;
    }
    let payload = serde_json::to_string_pretty(cfg)?;
    fs::write(path, payload)
        .with_context(|| format!("failed to write user config at {}", path.display()))
}
