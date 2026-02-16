use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct BlockTemplateResponse {
    pub block: TemplateBlock,
    pub target: String,
    pub header_base: String,
    #[serde(default)]
    pub template_id: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TemplateBlock {
    pub header: TemplateHeader,
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TemplateHeader {
    #[serde(default)]
    pub nonce: Option<u64>,
    #[serde(default, rename = "Nonce")]
    pub nonce_upper: Option<u64>,
    #[serde(default)]
    pub height: Option<u64>,
    #[serde(default, rename = "Height")]
    pub height_upper: Option<u64>,
    #[serde(default)]
    pub difficulty: Option<u64>,
    #[serde(default, rename = "Difficulty")]
    pub difficulty_upper: Option<u64>,
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

#[derive(Debug, Deserialize)]
pub struct SubmitBlockResponse {
    pub accepted: bool,
    #[allow(dead_code)]
    pub hash: Option<String>,
    pub height: Option<u64>,
}

pub fn decode_hex(input: &str, field_name: &str) -> Result<Vec<u8>> {
    let clean = input.trim();
    if clean.is_empty() {
        bail!("{field_name} is empty");
    }
    hex::decode(clean).with_context(|| format!("failed to decode {field_name} hex"))
}

pub fn parse_target(target_hex: &str) -> Result<[u8; 32]> {
    let bytes = decode_hex(target_hex, "target")?;
    if bytes.len() != 32 {
        bail!("target must be 32 bytes, got {}", bytes.len());
    }
    let mut target = [0u8; 32];
    target.copy_from_slice(&bytes);
    Ok(target)
}

pub fn hash_meets_target(hash: &[u8; 32], target: &[u8; 32]) -> bool {
    hash <= target
}

pub fn set_block_nonce(block: &mut TemplateBlock, nonce: u64) {
    if block.header.nonce.is_some() || block.header.nonce_upper.is_none() {
        block.header.nonce = Some(nonce);
        block.header.nonce_upper = None;
    } else {
        block.header.nonce_upper = Some(nonce);
        block.header.nonce = None;
    }
}

pub fn template_height(block: &TemplateBlock) -> Option<u64> {
    block.header.height.or(block.header.height_upper)
}

pub fn template_difficulty(block: &TemplateBlock) -> Option<u64> {
    block.header.difficulty.or(block.header.difficulty_upper)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn hash_meets_target_big_endian_compare() {
        let hash = [
            0x00u8, 0x01, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
            0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
            0xFF, 0xFF, 0xFF, 0xFF,
        ];
        let target = [
            0x00u8, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00,
        ];
        assert!(hash_meets_target(&hash, &target));
    }

    #[test]
    fn set_block_nonce_supports_lower_and_upper_keys() {
        let mut lower: TemplateBlock = serde_json::from_value(json!({
            "header": {"nonce": 0u64},
            "txns": []
        }))
        .expect("lower block should deserialize");
        set_block_nonce(&mut lower, 42);
        let out = serde_json::to_value(lower).expect("lower block should serialize");
        assert_eq!(out["header"]["nonce"].as_u64(), Some(42));

        let mut upper: TemplateBlock = serde_json::from_value(json!({
            "header": {"Nonce": 0u64},
            "txns": []
        }))
        .expect("upper block should deserialize");
        set_block_nonce(&mut upper, 9);
        let out = serde_json::to_value(upper).expect("upper block should serialize");
        assert_eq!(out["header"]["Nonce"].as_u64(), Some(9));
    }

    #[test]
    fn parse_target_requires_32_bytes() {
        let err = parse_target("00ff").expect_err("short target should fail");
        assert!(format!("{err:#}").contains("target must be 32 bytes"));
    }

    #[test]
    fn template_block_requires_header() {
        let err = serde_json::from_value::<TemplateBlock>(json!({"not_header": {}}))
            .expect_err("missing header should fail");
        assert!(err.to_string().contains("missing field `header`"));
    }
}
