use curve25519_dalek::ristretto::CompressedRistretto;
use sha3::{Digest, Sha3_256};

const STEALTH_ADDRESS_CHECKSUM_TAG: &[u8] = b"blocknet_stealth_address_checksum";
const NETWORK_ID_MAINNET: &str = "blocknet_mainnet";
const NETWORK_ID_TESTNET: &str = "blocknet_testnet";

pub(crate) fn validate_mining_address(address: &str) -> Result<(), String> {
    let trimmed = address.trim();
    if trimmed.is_empty() {
        return Err("address is required".to_string());
    }

    let decoded = bs58::decode(trimmed)
        .into_vec()
        .map_err(|_| "invalid base58 address".to_string())?;

    match decoded.len() {
        68 => {
            let payload = &decoded[..64];
            let checksum = &decoded[64..];
            if !checksum_matches(payload, checksum, NETWORK_ID_MAINNET)
                && !checksum_matches(payload, checksum, NETWORK_ID_TESTNET)
            {
                return Err("invalid address checksum".to_string());
            }
            validate_stealth_public_keys(payload)
        }
        len => Err(format!(
            "invalid address length: expected 68 bytes, got {len}"
        )),
    }
}

fn validate_stealth_public_keys(payload: &[u8]) -> Result<(), String> {
    if payload.len() != 64 {
        return Err("invalid address length".to_string());
    }

    if CompressedRistretto::from_slice(&payload[..32])
        .map_err(|_| "invalid address spend public key".to_string())?
        .decompress()
        .is_none()
    {
        return Err("invalid address spend public key".to_string());
    }

    if CompressedRistretto::from_slice(&payload[32..64])
        .map_err(|_| "invalid address view public key".to_string())?
        .decompress()
        .is_none()
    {
        return Err("invalid address view public key".to_string());
    }

    Ok(())
}

fn checksum_matches(payload: &[u8], checksum: &[u8], network_id: &str) -> bool {
    if payload.len() != 64 || checksum.len() != 4 {
        return false;
    }
    let sum = address_checksum(payload, network_id);
    checksum[0] == sum[0] && checksum[1] == sum[1] && checksum[2] == sum[2] && checksum[3] == sum[3]
}

fn address_checksum(payload: &[u8], network_id: &str) -> [u8; 32] {
    let mut hasher = Sha3_256::new();
    hasher.update(STEALTH_ADDRESS_CHECKSUM_TAG);
    hasher.update(network_id.as_bytes());
    hasher.update(payload);
    hasher.finalize().into()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dev_fee::DEV_ADDRESS;

    const CHECKSUM_VALID_INVALID_RISTRETTO_ADDRESS: &str =
        "S7YPHt98NDKrUNmFaHa9GQu4XJvRPkTR51bxdE4122UFxfB4cqdFP5R2pkJSrNTQGwmFVmKzKodu7F8XmHjTTx9PNx3i";

    #[test]
    fn validate_mining_address_accepts_dev_address() {
        validate_mining_address(DEV_ADDRESS).expect("dev fee address should be valid");
    }

    #[test]
    fn validate_mining_address_rejects_checksum_valid_invalid_ristretto_keys() {
        let err = validate_mining_address(CHECKSUM_VALID_INVALID_RISTRETTO_ADDRESS)
            .expect_err("invalid Ristretto address should be rejected");
        assert!(
            err.contains("invalid address spend public key")
                || err.contains("invalid address view public key"),
            "unexpected error: {err}"
        );
    }
}
