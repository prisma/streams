// Reproduce the fleet's pg_connect failure with the identical stack.
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let url = std::env::args().nth(1).expect("usage: pgtest <url>");
    let mut roots = rustls::RootCertStore::empty();
    roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    let tls = tokio_postgres_rustls::MakeRustlsConnect::new(
        rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth(),
    );
    match tokio_postgres::connect(&url, tls).await {
        Ok((client, conn)) => {
            tokio::spawn(conn);
            let row = client.query_one("SELECT count(*) FROM probe", &[]).await?;
            let n: i64 = row.get(0);
            println!("CONNECT OK; probe rows = {n}");
        }
        Err(e) => println!("CONNECT FAILED: {e} | source: {:?}", std::error::Error::source(&e)),
    }
    Ok(())
}
