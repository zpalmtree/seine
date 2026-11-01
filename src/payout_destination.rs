use anyhow::{bail, Result};
use sha3::{Digest, Sha3_256};

pub const CAP_PAYOUT_MEMO: &str = "payout_memo_v1";

pub fn validate_memo(memo: &str) -> Result<()> {
    if memo.is_empty() || memo.len() > 124 || memo.chars().any(char::is_control) {
        bail!("payout memo must contain 1–124 UTF-8 bytes and no control characters");
    }
    Ok(())
}

pub fn account_id(address: &str, memo: Option<&str>) -> String {
    let address = address.trim();
    match memo {
        None => address.to_string(),
        Some(memo) => {
            let mut hash = Sha3_256::new();
            hash.update(b"blocknet_pool_payout_memo_v1\0");
            hash.update((address.len() as u64).to_be_bytes());
            hash.update(address.as_bytes());
            hash.update(memo.as_bytes());
            format!("memo:{}", hex::encode(hash.finalize()))
        }
    }
}

pub fn format_amount(atomic: u64) -> String {
    let whole = atomic / 100_000_000;
    let fraction = atomic % 100_000_000;
    if fraction == 0 {
        whole.to_string()
    } else {
        format!("{whole}.{fraction:08}")
            .trim_end_matches('0')
            .to_string()
    }
}

pub fn known_exchange(address: &str) -> Option<&'static str> {
    match address.trim() {
        "ZJZqGfDPCVg78CRH5Z33CtHafg911Dp5Bauf1Dnxg5jgXx7Whqsvqka96FCV8W6QZXNDKzJefkFU6bwDEGBewqPrTWjij" => Some("SafeTrade"),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn miner_pool_account_contract() {
        assert_eq!(
            super::account_id("address", Some("ID001")),
            "memo:f7b26087161089e8313c0f4bc60341a65cc00f7bacdac6d4e5db48bb80ae2ee6"
        );
        assert_eq!(super::account_id(" address ", None), "address");
        assert!(super::validate_memo(&"é".repeat(63)).is_err());
        assert!(super::validate_memo("line\nbreak").is_err());
    }
}
