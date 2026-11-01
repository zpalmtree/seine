//! Minimal SOCKS5 client (RFC 1928, RFC 1929) for pool connections. The pool
//! hostname is sent to the proxy unresolved, so DNS lookups happen there.

use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// Covers proxy connect plus handshake; Tor circuits can take several seconds.
const SOCKS_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(20);

const VERSION: u8 = 0x05;
const METHOD_NO_AUTH: u8 = 0x00;
const METHOD_USER_PASS: u8 = 0x02;
const METHOD_NONE_ACCEPTABLE: u8 = 0xFF;
const CMD_CONNECT: u8 = 0x01;
const ATYP_IPV4: u8 = 0x01;
const ATYP_DOMAIN: u8 = 0x03;
const ATYP_IPV6: u8 = 0x04;

#[derive(Clone, PartialEq, Eq)]
pub struct SocksProxy {
    authority: String,
    credentials: Option<(String, String)>,
}

impl std::fmt::Debug for SocksProxy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.display())
    }
}

impl SocksProxy {
    /// Parses `socks5://[user:pass@]host:port`, `socks5h://…` or bare
    /// `host:port`.
    pub fn parse(input: &str) -> Result<Self> {
        let trimmed = input.trim().trim_end_matches('/');
        let rest = ["socks5h://", "socks5://"]
            .iter()
            .find_map(|scheme| trimmed.strip_prefix(scheme))
            .unwrap_or(trimmed);
        if rest.contains("://") {
            bail!("pool proxy must be socks5://host:port");
        }
        let (credentials, authority) = match rest.rsplit_once('@') {
            Some((userinfo, authority)) => {
                let (user, pass) = userinfo.split_once(':').unwrap_or((userinfo, ""));
                if user.is_empty() || user.len() > 255 || pass.len() > 255 {
                    bail!("pool proxy username must be 1-255 bytes and password at most 255");
                }
                (Some((user.to_string(), pass.to_string())), authority)
            }
            None => (None, rest),
        };
        let authority = authority.trim();
        let (host, port) = authority
            .rsplit_once(':')
            .ok_or_else(|| anyhow!("pool proxy must include a port: {authority}"))?;
        if host.trim_matches(['[', ']']).is_empty() || port.parse::<u16>().is_err() {
            bail!("invalid pool proxy address {authority}");
        }
        Ok(Self {
            authority: authority.to_string(),
            credentials,
        })
    }

    /// The same proxy for HTTP clients, with hostnames resolved by the proxy.
    pub fn reqwest_proxy(&self) -> Result<reqwest::Proxy> {
        let mut url = reqwest::Url::parse(&format!("socks5h://{}", self.authority))
            .map_err(|err| anyhow!("invalid pool proxy address {}: {err}", self.authority))?;
        if let Some((user, pass)) = &self.credentials {
            url.set_username(user)
                .and_then(|()| url.set_password(Some(pass)))
                .map_err(|()| anyhow!("invalid pool proxy credentials"))?;
        }
        reqwest::Proxy::all(url).map_err(|err| anyhow!("invalid pool proxy: {err}"))
    }

    /// The proxy address without credentials.
    pub fn display(&self) -> String {
        format!("socks5://{}", self.authority)
    }

    /// Opens a tunnel through the proxy to `host:port`.
    pub async fn connect(&self, host: &str, port: u16) -> Result<TcpStream> {
        tokio::time::timeout(SOCKS_HANDSHAKE_TIMEOUT, self.connect_inner(host, port))
            .await
            .map_err(|_| anyhow!("SOCKS5 proxy {} timed out", self.display()))?
            .map_err(|err| {
                if format!("{err:#}").contains("SOCKS5 proxy") {
                    err
                } else {
                    err.context(format!(
                        "SOCKS5 proxy {} closed the connection",
                        self.display()
                    ))
                }
            })
    }

    async fn connect_inner(&self, host: &str, port: u16) -> Result<TcpStream> {
        let mut stream = TcpStream::connect(&self.authority)
            .await
            .with_context(|| format!("SOCKS5 proxy {} unreachable", self.display()))?;
        let _ = stream.set_nodelay(true);

        let methods: &[u8] = if self.credentials.is_some() {
            &[METHOD_NO_AUTH, METHOD_USER_PASS]
        } else {
            &[METHOD_NO_AUTH]
        };
        let mut greeting = vec![VERSION, methods.len() as u8];
        greeting.extend_from_slice(methods);
        stream.write_all(&greeting).await?;
        let mut choice = [0u8; 2];
        stream.read_exact(&mut choice).await?;
        if choice[0] != VERSION {
            bail!("SOCKS5 proxy {} is not a SOCKS5 server", self.display());
        }
        match (choice[1], self.credentials.as_ref()) {
            (METHOD_NO_AUTH, _) => {}
            (METHOD_USER_PASS, Some((user, pass))) => {
                let mut auth = vec![0x01, user.len() as u8];
                auth.extend_from_slice(user.as_bytes());
                auth.push(pass.len() as u8);
                auth.extend_from_slice(pass.as_bytes());
                stream.write_all(&auth).await?;
                let mut status = [0u8; 2];
                stream.read_exact(&mut status).await?;
                if status[1] != 0 {
                    bail!(
                        "SOCKS5 proxy {} rejected the username or password",
                        self.display()
                    );
                }
            }
            (METHOD_NONE_ACCEPTABLE, _) | (METHOD_USER_PASS, None) => {
                bail!(
                    "SOCKS5 proxy {} requires a username and password",
                    self.display()
                );
            }
            (other, _) => bail!(
                "SOCKS5 proxy {} chose unsupported auth method {other}",
                self.display()
            ),
        }

        let mut request = vec![VERSION, CMD_CONNECT, 0x00];
        match host.parse::<std::net::IpAddr>() {
            Ok(std::net::IpAddr::V4(ip)) => {
                request.push(ATYP_IPV4);
                request.extend_from_slice(&ip.octets());
            }
            Ok(std::net::IpAddr::V6(ip)) => {
                request.push(ATYP_IPV6);
                request.extend_from_slice(&ip.octets());
            }
            Err(_) => {
                if host.len() > 255 {
                    bail!("pool hostname too long for SOCKS5");
                }
                request.push(ATYP_DOMAIN);
                request.push(host.len() as u8);
                request.extend_from_slice(host.as_bytes());
            }
        }
        request.extend_from_slice(&port.to_be_bytes());
        stream.write_all(&request).await?;

        let mut reply = [0u8; 4];
        stream.read_exact(&mut reply).await?;
        if reply[1] != 0 {
            bail!(
                "SOCKS5 proxy {} could not reach the pool: {}",
                self.display(),
                reply_message(reply[1])
            );
        }
        let bound_len = match reply[3] {
            ATYP_IPV4 => 4,
            ATYP_IPV6 => 16,
            ATYP_DOMAIN => stream.read_u8().await? as usize,
            other => bail!("SOCKS5 proxy sent unknown address type {other}"),
        };
        let mut bound = vec![0u8; bound_len + 2];
        stream.read_exact(&mut bound).await?;
        Ok(stream)
    }
}

fn reply_message(code: u8) -> &'static str {
    match code {
        0x01 => "general failure",
        0x02 => "connection not allowed by ruleset",
        0x03 => "network unreachable",
        0x04 => "host unreachable",
        0x05 => "connection refused",
        0x06 => "TTL expired",
        0x07 => "command not supported",
        0x08 => "address type not supported",
        _ => "unknown error",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_schemes_credentials_and_hides_them_in_display() {
        let proxy = SocksProxy::parse("socks5://alice:s3cret@127.0.0.1:1080").unwrap();
        assert_eq!(proxy.display(), "socks5://127.0.0.1:1080");
        assert_eq!(format!("{proxy:?}"), "socks5://127.0.0.1:1080");
        assert_eq!(
            proxy.credentials,
            Some(("alice".to_string(), "s3cret".to_string()))
        );
        assert_eq!(
            SocksProxy::parse("socks5h://proxy.example:9050")
                .unwrap()
                .display(),
            "socks5://proxy.example:9050"
        );
        assert!(SocksProxy::parse("proxy.example:9050").is_ok());
        assert!(SocksProxy::parse("http://proxy.example:8080").is_err());
        assert!(SocksProxy::parse("proxy.example").is_err());
        assert!(SocksProxy::parse("socks5://:1080").is_err());
    }
}
