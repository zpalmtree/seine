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

/// The exchange behind a shared deposit address, with or without a `solo:` prefix.
pub fn known_exchange(address: &str) -> Option<&'static str> {
    match crate::shortname::split_solo(address).1 {
        "ZJZqGfDPCVg78CRH5Z33CtHafg911Dp5Bauf1Dnxg5jgXx7Whqsvqka96FCV8W6QZXNDKzJefkFU6bwDEGBewqPrTWjij" => Some("SafeTrade"),
        _ => None,
    }
}

/// Refuses a resolved destination that is a shared exchange address with no
/// memo, or one that is mined somewhere memos can't reach.
pub fn ensure_exchange_memo(address: &str, memo: Option<&str>, direct_pool: bool) -> Result<()> {
    let Some(exchange) = known_exchange(address) else {
        return Ok(());
    };
    if !direct_pool {
        bail!("{exchange} uses a shared deposit address that needs memo payouts through a direct pool connection");
    }
    if memo.is_none() {
        bail!("{exchange} uses a shared deposit address; use --payout-memo with the memo from your deposit page");
    }
    Ok(())
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

    #[test]
    fn exchange_addresses_need_a_memo_on_a_direct_pool() {
        let exchange = "ZJZqGfDPCVg78CRH5Z33CtHafg911Dp5Bauf1Dnxg5jgXx7Whqsvqka96FCV8W6QZXNDKzJefkFU6bwDEGBewqPrTWjij";
        for address in [exchange.to_string(), format!("Solo:{exchange}")] {
            assert_eq!(super::known_exchange(&address), Some("SafeTrade"));
            assert!(super::ensure_exchange_memo(&address, None, true).is_err());
            assert!(super::ensure_exchange_memo(&address, Some("ID1"), false).is_err());
            assert!(super::ensure_exchange_memo(&address, Some("ID1"), true).is_ok());
        }
        assert!(super::ensure_exchange_memo("address", None, false).is_ok());
    }
}
