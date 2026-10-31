//! Transport checks use fixture certificates and never start mining workers.

use super::*;
use rcgen::{generate_simple_self_signed, CertifiedKey};
use tokio::net::TcpListener;
use tokio_rustls::rustls::{crypto::ring, pki_types::PrivatePkcs8KeyDer, ServerConfig};
use tokio_rustls::TlsAcceptor;

fn certificate() -> CertifiedKey {
    generate_simple_self_signed(vec!["localhost".into()]).unwrap()
}

#[tokio::test]
async fn tls_hostname_rejects_untrusted_certificate_but_ip_uses_encryption_only() {
    let _diagnostics_lock = crate::diagnostics::TEST_LOCK.lock().unwrap();
    let CertifiedKey { cert, key_pair } = certificate();
    let config = ServerConfig::builder_with_provider(Arc::new(ring::default_provider()))
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(
            vec![cert.der().clone()],
            PrivatePkcs8KeyDer::from(key_pair.serialize_der()).into(),
        )
        .unwrap();
    let acceptor = TlsAcceptor::from(Arc::new(config));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (tcp, _) = listener.accept().await.unwrap();
        assert!(acceptor.accept(tcp).await.is_err());
        let (tcp, _) = listener.accept().await.unwrap();
        let mut stream = acceptor.accept(tcp).await.unwrap();
        stream.write_all(b"encrypted reply\n").await.unwrap();
    });
    // Connect TCP explicitly to IPv4 while keeping the endpoint's hostname for
    // certificate verification; localhost may otherwise resolve to IPv6 first.
    let hostname =
        parse_pool_endpoint(&format!("stratum+ssl://localhost:{}", address.port())).unwrap();
    let error = connect_pool_tls(&hostname, TokioTcpStream::connect(address).await.unwrap())
        .await
        .unwrap_err();
    assert!(error.to_string().contains("UnknownIssuer"), "{error:#}");

    let ip = parse_pool_endpoint(&format!("stratum+ssl://{address}")).unwrap();
    let mut connection = connect_pool_transport(&ip).await.unwrap();
    let frame = tokio::time::timeout(Duration::from_secs(5), connection.inbound_rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(frame, InboundFrame::Text(text) if text == "encrypted reply"));
    connection.abort();
    tokio::time::timeout(Duration::from_secs(5), server)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn tls_endpoint_does_not_fall_back_to_plaintext() {
    let _diagnostics_lock = crate::diagnostics::TEST_LOCK.lock().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (mut tcp, _) = listener.accept().await.unwrap();
        tcp.write_all(b"{\"status\":\"ok\"}\n").await.unwrap();
        let mut first = [0; 1];
        tcp.read_exact(&mut first).await.unwrap();
        assert_eq!(first[0], 0x16, "client must send a TLS handshake");
    });
    let endpoint = parse_pool_endpoint(&format!("stratum+ssl://{address}")).unwrap();
    let error = connect_pool_transport(&endpoint).await.err().unwrap();
    assert!(
        error.to_string().contains("TLS handshake failed"),
        "{error:#}"
    );
    server.await.unwrap();
}

/// Opt-in cross-repo check against a real blocknet-pool gateway executable.
/// The engine is a fixture: this verifies transport, login, jobs, submits and
/// reconnects, not PoW validation. All ports are loopback and ephemeral.
#[cfg(unix)]
mod gateway {
    use super::*;
    use std::io::{Read, Write};
    use std::os::unix::net::UnixStream;
    use std::process::{Child, Command, Stdio};

    struct GatewayProcess(Child);
    impl Drop for GatewayProcess {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }

    // Gateway wire contract: [kind u8][connection u64 BE][length u32 BE][JSON].
    fn send(engine: &mut UnixStream, kind: u8, connection: u64, payload: &str) {
        let mut frame = vec![kind];
        frame.extend_from_slice(&connection.to_be_bytes());
        frame.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        frame.extend_from_slice(payload.as_bytes());
        engine.write_all(&frame).unwrap();
    }

    fn receive(engine: &mut UnixStream) -> (u8, u64, String) {
        let mut header = [0; 13];
        engine.read_exact(&mut header).unwrap();
        let size = u32::from_be_bytes(header[9..13].try_into().unwrap()) as usize;
        assert!(size <= 1 << 20);
        let mut body = vec![0; size];
        engine.read_exact(&mut body).unwrap();
        (
            header[0],
            u64::from_be_bytes(header[1..9].try_into().unwrap()),
            String::from_utf8(body).unwrap(),
        )
    }

    #[test]
    #[ignore = "set SEINE_TEST_GATEWAY_BIN to a built blocknet-pool-gateway executable"]
    fn tls_gateway_login_job_submit_and_reconnect() {
        let binary = std::env::var_os("SEINE_TEST_GATEWAY_BIN").expect("SEINE_TEST_GATEWAY_BIN");
        let dir = tempfile::tempdir().unwrap();
        let CertifiedKey { cert, key_pair } = certificate();
        let cert_path = dir.path().join("cert.pem");
        let key_path = dir.path().join("key.pem");
        let socket_path = dir.path().join("gateway.sock");
        std::fs::write(&cert_path, cert.pem()).unwrap();
        std::fs::write(&key_path, key_pair.serialize_pem()).unwrap();
        let reserved = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = reserved.local_addr().unwrap().port();
        let config = serde_json::json!({
            "stratum_host": "127.0.0.1", "stratum_port": 0,
            "gateway_metrics_addr": "127.0.0.1:0", "gateway_max_miners": 8,
            "gateway_socket": socket_path, "gateway_tls_port": port,
            "gateway_tls_cert": cert_path, "gateway_tls_key": key_path
        });
        let config_path = dir.path().join("config.json");
        std::fs::write(&config_path, config.to_string()).unwrap();
        drop(reserved);
        let mut gateway = GatewayProcess(
            Command::new(binary)
                .arg("--config")
                .arg(&config_path)
                .stdout(Stdio::null())
                .spawn()
                .unwrap(),
        );
        let deadline = Instant::now() + Duration::from_secs(10);
        let mut engine = loop {
            assert!(gateway.0.try_wait().unwrap().is_none(), "gateway exited");
            if let Ok(engine) = UnixStream::connect(&socket_path) {
                break engine;
            }
            assert!(Instant::now() < deadline, "gateway did not start");
            std::thread::sleep(Duration::from_millis(20));
        };
        engine
            .set_read_timeout(Some(Duration::from_secs(10)))
            .unwrap();
        engine
            .set_write_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        send(
            &mut engine,
            1,
            0,
            r#"{"instance":"tls-test","pid":1,"started_at_ms":0}"#,
        );
        let (kind, _, role) = receive(&mut engine);
        assert_eq!((kind, role.as_str()), (2, "primary"));
        let fixture = std::thread::spawn(move || {
            let mut logins = 0;
            let mut submits = 0;
            while submits < 2 {
                let (kind, conn, body) = receive(&mut engine);
                if kind != 4 {
                    continue;
                }
                let request: Value = serde_json::from_str(&body).unwrap();
                match request["method"].as_str().unwrap() {
                    "login" => {
                        logins += 1;
                        send(&mut engine, 4, conn, &serde_json::json!({
                            "id": request["id"], "status": "ok",
                            "result": {"protocol_version": 2, "capabilities": ["submit_claimed_hash"]}
                        }).to_string());
                        send(&mut engine, 4, conn, &serde_json::json!({
                            "method": "job", "params": {
                                "job_id": format!("tls-{logins}"), "header_base": "00".repeat(92),
                                "target": "ff".repeat(32), "height": 10,
                                "nonce_start": 0, "nonce_end": 99
                            }
                        }).to_string());
                    }
                    "submit" => {
                        assert_eq!(request["params"]["job_id"], format!("tls-{logins}"));
                        assert_eq!(request["params"]["nonce"], 7);
                        submits += 1;
                        send(
                            &mut engine,
                            4,
                            conn,
                            &serde_json::json!({
                                "id": request["id"], "result": {"accepted": true}
                            })
                            .to_string(),
                        );
                        if submits == 1 {
                            send(&mut engine, 5, conn, "");
                        }
                    }
                    "keepalived" => {}
                    other => panic!("unexpected request: {other}"),
                }
            }
            assert_eq!(logins, 2);
            // Keep the engine alive until the client has read its final ACK.
            engine
        });
        let client = PoolClient::connect(
            &format!("stratum+ssl://127.0.0.1:{port}"),
            "BTestAddress".into(),
            "tls-fixture".into(),
            Arc::new(AtomicBool::new(false)),
        )
        .unwrap();
        let deadline = Instant::now() + Duration::from_secs(20);
        let (mut logins, mut accepted) = (0, 0);
        while accepted < 2 {
            assert!(Instant::now() < deadline, "TLS round trip timed out");
            match client.recv_event_timeout(Duration::from_millis(100)) {
                Some(PoolEvent::LoginAccepted(_)) => logins += 1,
                Some(PoolEvent::Job(job)) => {
                    client.submit_share(job.job_id, 7, Some([0; 32])).unwrap()
                }
                Some(PoolEvent::SubmitAck(ack)) => {
                    assert!(ack.accepted, "{ack:?}");
                    accepted += 1;
                }
                _ => {}
            }
        }
        assert_eq!(logins, 2);
        drop(client);
        drop(fixture.join().unwrap());
    }
}
