//! Blocknet ID wire contract: blocknet-core/handle_resolver.go.
//! Resolve only at session startup; accounting always uses the validated address.
use std::io::Read;
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use base64::{engine::general_purpose::STANDARD, Engine};
use ring::signature::{UnparsedPublicKey, ED25519};
use serde::de::DeserializeOwned;
use serde::Deserialize;

use crate::address::validate_mining_address as validate_address;

const MAX_RESPONSE_BYTES: u64 = 16 * 1024;

pub(crate) fn normalize_shortname(input: &str) -> Result<Option<String>, String> {
    let input = input.trim();
    if !input.starts_with(['@', '$']) {
        return Ok(None);
    }
    let handle = input[1..].to_ascii_lowercase();
    if handle.is_empty() || handle.len() > 64 {
        return Err("shortname must contain 1 to 64 characters".to_string());
    }
    if !handle
        .bytes()
        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || b"_.-".contains(&c))
    {
        return Err(
            "invalid shortname: use letters, digits, underscore, hyphen or dot".to_string(),
        );
    }
    Ok(Some(handle))
}

/// Login prefix that asks the pool to mine solo for this destination.
pub(crate) const SOLO_PREFIX: &str = "solo:";

/// Splits an optional `solo:` prefix (any letter case) off a destination,
/// returning whether it was present and the trimmed remainder.
pub(crate) fn split_solo(input: &str) -> (bool, &str) {
    let trimmed = input.trim();
    match trimmed.get(..SOLO_PREFIX.len()) {
        Some(prefix) if prefix.eq_ignore_ascii_case(SOLO_PREFIX) => {
            (true, trimmed[SOLO_PREFIX.len()..].trim())
        }
        _ => (false, trimmed),
    }
}

/// `destination` with the canonical `solo:` prefix when `solo` is set.
pub(crate) fn with_solo(solo: bool, destination: &str) -> String {
    if solo {
        format!("{SOLO_PREFIX}{destination}")
    } else {
        destination.to_string()
    }
}

/// Accepts an address or shortname, optionally prefixed with `solo:`.
pub(crate) fn validate_destination(input: &str) -> Result<(), String> {
    let (solo, destination) = split_solo(input);
    if solo && destination.is_empty() {
        return Err("solo: must be followed by an address or shortname".to_string());
    }
    match normalize_shortname(destination)? {
        Some(_) => Ok(()),
        None => validate_address(destination),
    }
}

pub(crate) struct ShortnameResolver {
    base_url: String,
}

impl Default for ShortnameResolver {
    fn default() -> Self {
        Self {
            base_url: "https://blocknet.id".to_string(),
        }
    }
}

#[derive(Deserialize)]
struct Resolution {
    address: String,
    #[serde(default)]
    handle: String,
    payload: String,
    sig: String,
    #[serde(default)]
    updated_at: i64,
}

#[derive(Deserialize)]
struct SignedPayload {
    handle: String,
    address: String,
    updated_at_unix: i64,
}

#[derive(Deserialize)]
struct SigningKey {
    signing_pubkey: String,
}

impl ShortnameResolver {
    // Blocking HTTP: call from a mining thread or Tokio's blocking pool.
    /// Resolves a shortname to its address, keeping a `solo:` prefix.
    pub(crate) fn resolve(&self, input: &str) -> Result<String> {
        validate_destination(input).map_err(|err| anyhow!(err))?;
        let (solo, destination) = split_solo(input);
        Ok(with_solo(solo, &self.resolve_destination(destination)?))
    }

    fn resolve_destination(&self, input: &str) -> Result<String> {
        let Some(handle) = normalize_shortname(input).map_err(|err| anyhow!(err))? else {
            return Ok(input.trim().to_string());
        };
        let client = reqwest::blocking::Client::builder()
            .timeout(Duration::from_secs(3))
            .redirect(reqwest::redirect::Policy::none())
            .build()?;
        let resolved: Resolution = get_json(
            &client,
            &format!("{}/api/v1/resolve/{handle}", self.base_url),
        )
        .with_context(|| format!("could not resolve shortname @{handle}"))?;
        let key: SigningKey = get_json(
            &client,
            &format!("{}/.well-known/blocknet-id.json", self.base_url),
        )
        .context("could not fetch shortname signing key")?;
        verify_resolution(&handle, &resolved, &key.signing_pubkey)?;
        validate_address(&resolved.address)
            .map_err(|err| anyhow!("shortname resolved to invalid address: {err}"))?;
        Ok(resolved.address.trim().to_string())
    }
}

fn get_json<T: DeserializeOwned>(client: &reqwest::blocking::Client, url: &str) -> Result<T> {
    let response = client
        .get(url)
        .send()
        .context("shortname service unavailable")?;
    if response.status() == reqwest::StatusCode::NOT_FOUND {
        bail!("shortname not found");
    }
    if response.status() != reqwest::StatusCode::OK {
        bail!("shortname service returned HTTP {}", response.status());
    }
    let mut body = Vec::new();
    response
        .take(MAX_RESPONSE_BYTES + 1)
        .read_to_end(&mut body)?;
    if body.len() as u64 > MAX_RESPONSE_BYTES {
        bail!("shortname response too large");
    }
    serde_json::from_slice(&body).context("invalid shortname response")
}

fn verify_resolution(handle: &str, resolved: &Resolution, public_key: &str) -> Result<()> {
    let public_key = STANDARD
        .decode(public_key.trim())
        .context("invalid shortname signing key")?;
    let signature = STANDARD
        .decode(&resolved.sig)
        .context("invalid shortname signature")?;
    UnparsedPublicKey::new(&ED25519, public_key)
        .verify(resolved.payload.as_bytes(), &signature)
        .map_err(|_| anyhow!("shortname signature verification failed"))?;
    let signed: SignedPayload =
        serde_json::from_str(&resolved.payload).context("invalid signed shortname payload")?;
    if !signed.handle.trim().eq_ignore_ascii_case(handle)
        || (!resolved.handle.is_empty()
            && !signed
                .handle
                .trim()
                .eq_ignore_ascii_case(resolved.handle.trim()))
    {
        bail!("shortname response handle mismatch");
    }
    if signed.address.trim() != resolved.address.trim() {
        bail!("shortname response address mismatch");
    }
    if resolved.updated_at > 0 && signed.updated_at_unix != resolved.updated_at {
        bail!("shortname response timestamp mismatch");
    }
    Ok(())
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use ring::signature::{Ed25519KeyPair, KeyPair};
    use serde_json::json;
    use std::io::{BufRead, BufReader, Write};
    use std::net::TcpListener;

    pub(crate) const ADDRESS: &str = "TCpDTBpjwaHcjWEXJwoT5MHrFpoi9cTuftzNLN7bNoBJjCSJEH5ZGXwNcUauhjgoV2ocF9RJWm8Ui2k1QAkAYrwMgG8Wk";

    fn signed_response(address: &str) -> (Resolution, String) {
        let key = Ed25519KeyPair::from_seed_unchecked(&[42; 32]).unwrap();
        let payload =
            json!({"handle": "alice", "address": address, "updated_at_unix": 123}).to_string();
        let sig = STANDARD.encode(key.sign(payload.as_bytes()).as_ref());
        (
            Resolution {
                address: address.into(),
                handle: "alice".into(),
                payload,
                sig,
                updated_at: 123,
            },
            STANDARD.encode(key.public_key().as_ref()),
        )
    }

    // The local HTTP fixture checks the actual path and serves a signed response.
    // No process-wide environment variables or live resolver dependencies.
    pub(crate) fn mock_resolver(address: &str) -> (ShortnameResolver, std::thread::JoinHandle<()>) {
        let (resolved, key) = signed_response(address);
        let response = json!({"address": resolved.address, "handle": resolved.handle,
            "payload": resolved.payload, "sig": resolved.sig, "updated_at": resolved.updated_at})
        .to_string();
        mock_http(vec![
            ("/api/v1/resolve/alice", 200, response),
            (
                "/.well-known/blocknet-id.json",
                200,
                json!({"signing_pubkey": key}).to_string(),
            ),
        ])
    }

    fn mock_http(
        responses: Vec<(&'static str, u16, String)>,
    ) -> (ShortnameResolver, std::thread::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let base_url = format!("http://{}", listener.local_addr().unwrap());
        listener.set_nonblocking(true).unwrap();
        let task = std::thread::spawn(move || {
            let deadline = std::time::Instant::now() + Duration::from_secs(10);
            for (path, status, body) in responses {
                let mut stream = loop {
                    match listener.accept() {
                        Ok((stream, _)) => break stream,
                        Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
                            assert!(
                                std::time::Instant::now() < deadline,
                                "resolver request never arrived"
                            );
                            std::thread::sleep(Duration::from_millis(5));
                        }
                        Err(err) => panic!("accept: {err}"),
                    }
                };
                stream
                    .set_read_timeout(Some(Duration::from_secs(3)))
                    .unwrap();
                let mut reader = BufReader::new(stream.try_clone().unwrap());
                let mut line = String::new();
                reader.read_line(&mut line).unwrap();
                assert_eq!(line.trim(), format!("GET {path} HTTP/1.1"));
                loop {
                    line.clear();
                    reader.read_line(&mut line).unwrap();
                    if line == "\r\n" || line.is_empty() {
                        break;
                    }
                }
                write!(stream, "HTTP/1.1 {status} Test\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).unwrap();
            }
        });
        (ShortnameResolver { base_url }, task)
    }

    #[test]
    fn shortname_syntax_and_raw_addresses() {
        for name in [" @Alice ", "$ALICE", "@a_1.b-c"] {
            assert!(validate_destination(name).is_ok(), "{name}");
        }
        assert_eq!(
            normalize_shortname(" $ALICE ").unwrap(),
            Some("alice".into())
        );
        for bad in [
            "@",
            "$",
            "@@alice",
            "@../alice",
            "@alice?x",
            "@álîce",
            "@a b",
        ] {
            assert!(validate_destination(bad).is_err(), "{bad}");
        }
        assert!(validate_destination(&format!("@{}", "a".repeat(65))).is_err());
        assert!(validate_destination("alice").is_err());
        let resolver = ShortnameResolver {
            base_url: "http://127.0.0.1:1".into(),
        };
        assert_eq!(resolver.resolve(&format!(" {ADDRESS} ")).unwrap(), ADDRESS);
        assert!(resolver.resolve("not-an-address").is_err());
    }

    #[test]
    fn shortname_resolves_both_prefixes_and_case() {
        for name in [" @Alice ", "$ALICE"] {
            let (resolver, task) = mock_resolver(ADDRESS);
            assert_eq!(resolver.resolve(name).unwrap(), ADDRESS);
            task.join().unwrap();
        }
    }

    #[test]
    fn solo_prefix_wraps_addresses_and_shortnames() {
        assert_eq!(split_solo(" SOLO: @Alice "), (true, "@Alice"));
        assert_eq!(split_solo(ADDRESS), (false, ADDRESS));
        for ok in [
            format!("solo:{ADDRESS}"),
            "Solo:@alice".into(),
            "solo: $ALICE".into(),
        ] {
            assert!(validate_destination(&ok).is_ok(), "{ok}");
        }
        for bad in ["solo:", "solo: ", "solo:alice", "solo:solo:@alice"] {
            assert!(validate_destination(bad).is_err(), "{bad}");
        }
        let offline = ShortnameResolver {
            base_url: "http://127.0.0.1:1".into(),
        };
        assert_eq!(
            offline.resolve(&format!("SOLO:{ADDRESS}")).unwrap(),
            format!("solo:{ADDRESS}")
        );
        let (resolver, task) = mock_resolver(ADDRESS);
        assert_eq!(
            resolver.resolve("solo:@Alice").unwrap(),
            format!("solo:{ADDRESS}")
        );
        task.join().unwrap();
    }

    #[test]
    fn shortname_rejects_tampering() {
        let (mut response, key) = signed_response(ADDRESS);
        verify_resolution("alice", &response, &key).unwrap();
        assert!(verify_resolution("bob", &response, &key).is_err());
        response.address = "different".into();
        assert!(verify_resolution("alice", &response, &key).is_err());
        response.address = ADDRESS.into();
        response.handle = "bob".into();
        assert!(verify_resolution("alice", &response, &key).is_err());
        response.handle = "alice".into();
        response.updated_at += 1;
        assert!(verify_resolution("alice", &response, &key).is_err());
        response.updated_at = 123;
        response.payload.push(' ');
        assert!(verify_resolution("alice", &response, &key).is_err());
        let (response, _) = signed_response(ADDRESS);
        assert!(verify_resolution("alice", &response, &STANDARD.encode([0; 32])).is_err());
    }

    #[test]
    fn shortname_rejects_signed_invalid_address() {
        for address in ["invalid", "S7YPHt98NDKrUNmFaHa9GQu4XJvRPkTR51bxdE4122UFxfB4cqdFP5R2pkJSrNTQGwmFVmKzKodu7F8XmHjTTx9PNx3i"] {
            let (resolver, task) = mock_resolver(address);
            assert!(resolver.resolve("@alice").unwrap_err().to_string().contains("invalid address"));
            task.join().unwrap();
        }
    }

    #[test]
    fn shortname_errors_fail_closed() {
        for (status, body) in [
            (404, "{}".into()),
            (503, "{}".into()),
            (200, "not json".into()),
            (200, "x".repeat(MAX_RESPONSE_BYTES as usize + 1)),
        ] {
            let (resolver, task) = mock_http(vec![("/api/v1/resolve/alice", status, body)]);
            assert!(resolver.resolve("@alice").is_err());
            task.join().unwrap();
        }
    }
}
