use anyhow::{anyhow, bail, Context, Result};
use serde::Deserialize;
use serde_json::{json, Value};

#[derive(Debug, Deserialize)]
pub struct BlockTemplateResponse {
    pub block: Value,
    pub target: String,
    pub header_base: String,
}

#[derive(Debug, Deserialize)]
pub struct SubmitBlockResponse {
    pub accepted: bool,
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
    for i in 0..32 {
        if hash[i] < target[i] {
            return true;
        }
        if hash[i] > target[i] {
            return false;
        }
    }
    true
}

pub fn set_block_nonce(block: &mut Value, nonce: u64) -> Result<()> {
    let header = block
        .get_mut("header")
        .and_then(Value::as_object_mut)
        .ok_or_else(|| anyhow!("template block is missing object field 'header'"))?;

    if header.contains_key("nonce") {
        header.insert("nonce".to_string(), json!(nonce));
        return Ok(());
    }

    header.insert("Nonce".to_string(), json!(nonce));
    Ok(())
}

pub fn template_height(block: &Value) -> Option<u64> {
    let header = block.get("header")?;
    header
        .get("height")
        .and_then(Value::as_u64)
        .or_else(|| header.get("Height").and_then(Value::as_u64))
}

pub fn template_difficulty(block: &Value) -> Option<u64> {
    let header = block.get("header")?;
    header
        .get("difficulty")
        .and_then(Value::as_u64)
        .or_else(|| header.get("Difficulty").and_then(Value::as_u64))
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
        let mut lower = json!({"header": {"nonce": 0u64}});
        set_block_nonce(&mut lower, 42).unwrap();
        assert_eq!(lower["header"]["nonce"].as_u64(), Some(42));

        let mut upper = json!({"header": {"Nonce": 0u64}});
        set_block_nonce(&mut upper, 9).unwrap();
        assert_eq!(upper["header"]["Nonce"].as_u64(), Some(9));
    }

    #[test]
    fn parse_target_requires_32_bytes() {
        let err = parse_target("00ff").unwrap_err();
        assert!(format!("{err:#}").contains("target must be 32 bytes"));
    }

    #[test]
    fn set_block_nonce_requires_header() {
        let mut bad = json!({"not_header": {}});
        let err = set_block_nonce(&mut bad, 7).unwrap_err();
        assert!(format!("{err:#}").contains("missing object field 'header'"));
    }
}
