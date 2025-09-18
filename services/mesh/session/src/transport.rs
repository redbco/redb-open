//! TCP and TLS transport for mesh sessions.
//!
//! This module provides both plain TCP and TLS transport capabilities,
//! with support for mTLS authentication and certificate-based node identity.

use std::net::SocketAddr;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::{TcpListener, TcpStream};
#[cfg(feature = "tls")]
use tracing::{info, debug};

/// Unified stream type that can be either plain TCP or TLS
pub enum IoStream {
    /// Plain TCP stream
    Plain(TcpStream),
    /// TLS-wrapped stream
    #[cfg(feature = "tls")]
    Tls(tokio_rustls::server::TlsStream<TcpStream>),
    /// TLS client stream
    #[cfg(feature = "tls")]
    TlsClient(tokio_rustls::client::TlsStream<TcpStream>),
}

impl AsyncRead for IoStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            IoStream::Plain(stream) => Pin::new(stream).poll_read(cx, buf),
            #[cfg(feature = "tls")]
            IoStream::Tls(stream) => Pin::new(stream).poll_read(cx, buf),
            #[cfg(feature = "tls")]
            IoStream::TlsClient(stream) => Pin::new(stream).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for IoStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, std::io::Error>> {
        match self.get_mut() {
            IoStream::Plain(stream) => Pin::new(stream).poll_write(cx, buf),
            #[cfg(feature = "tls")]
            IoStream::Tls(stream) => Pin::new(stream).poll_write(cx, buf),
            #[cfg(feature = "tls")]
            IoStream::TlsClient(stream) => Pin::new(stream).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), std::io::Error>> {
        match self.get_mut() {
            IoStream::Plain(stream) => Pin::new(stream).poll_flush(cx),
            #[cfg(feature = "tls")]
            IoStream::Tls(stream) => Pin::new(stream).poll_flush(cx),
            #[cfg(feature = "tls")]
            IoStream::TlsClient(stream) => Pin::new(stream).poll_flush(cx),
        }
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        match self.get_mut() {
            IoStream::Plain(stream) => Pin::new(stream).poll_shutdown(cx),
            #[cfg(feature = "tls")]
            IoStream::Tls(stream) => Pin::new(stream).poll_shutdown(cx),
            #[cfg(feature = "tls")]
            IoStream::TlsClient(stream) => Pin::new(stream).poll_shutdown(cx),
        }
    }
}

impl IoStream {
    /// Get the peer address of the underlying stream
    pub fn peer_addr(&self) -> std::io::Result<SocketAddr> {
        match self {
            IoStream::Plain(stream) => stream.peer_addr(),
            #[cfg(feature = "tls")]
            IoStream::Tls(stream) => stream.get_ref().0.peer_addr(),
            #[cfg(feature = "tls")]
            IoStream::TlsClient(stream) => stream.get_ref().0.peer_addr(),
        }
    }
}

/// Create a TCP listener bound to the given address
pub async fn listen_tcp(addr: SocketAddr) -> tokio::io::Result<TcpListener> {
    TcpListener::bind(addr).await
}

/// Connect to a TCP address
pub async fn connect_tcp(addr: SocketAddr) -> tokio::io::Result<TcpStream> {
    TcpStream::connect(addr).await
}

// TLS-specific functionality
#[cfg(feature = "tls")]
/// TLS transport layer implementation for secure mesh communication
pub mod tls {
    use super::*;
    use anyhow::{Context as AnyhowContext, Result};
    use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
    use rustls::{ClientConfig, RootCertStore, ServerConfig};
    use std::sync::Arc;
    use tokio_rustls::{TlsAcceptor, TlsConnector};

    /// TLS server acceptor wrapper
    pub struct TlsServer {
        acceptor: TlsAcceptor,
    }

    /// TLS client connector wrapper
    #[allow(dead_code)]
    pub struct TlsClient {
        connector: TlsConnector,
    }

    /// Create a TLS server configuration with mTLS
    pub fn make_server_config(
        cert_chain_pem: &str,
        private_key_pem: &str,
        ca_pem: &str,
    ) -> Result<ServerConfig> {
        info!("Creating TLS server configuration with mTLS");

        // Install default crypto provider if not already set
        let _ = rustls::crypto::ring::default_provider().install_default();

        // Load server certificate chain
        let cert_results: Result<Vec<_>, _> =
            rustls_pemfile::certs(&mut cert_chain_pem.as_bytes()).collect();
        let certs = cert_results
            .context("Failed to parse certificate chain")?
            .into_iter()
            .map(CertificateDer::from)
            .collect::<Vec<_>>();

        if certs.is_empty() {
            anyhow::bail!("No certificates found in certificate chain");
        }

        // Load private key
        let key = {
            let key_results: Result<Vec<_>, _> =
                rustls_pemfile::pkcs8_private_keys(&mut private_key_pem.as_bytes()).collect();
            let mut keys = key_results.context("Failed to parse private key")?;
            if keys.is_empty() {
                anyhow::bail!("No private key found");
            }
            PrivateKeyDer::from(keys.remove(0))
        };

        // Load CA certificates for client verification
        let mut roots = RootCertStore::empty();
        let ca_results: Result<Vec<_>, _> = rustls_pemfile::certs(&mut ca_pem.as_bytes()).collect();
        let ca_certs = ca_results.context("Failed to parse CA certificates")?;

        for ca_cert in ca_certs {
            roots
                .add(CertificateDer::from(ca_cert))
                .context("Failed to add CA certificate to root store")?;
        }

        // Create client certificate verifier for mTLS
        let client_verifier = rustls::server::WebPkiClientVerifier::builder(Arc::new(roots))
            .build()
            .context("Failed to build client certificate verifier")?;

        // Build server configuration with client certificate verification (mTLS)
        let mut config = ServerConfig::builder()
            .with_client_cert_verifier(client_verifier)
            .with_single_cert(certs, key)
            .context("Failed to configure server certificate")?;

        // Set ALPN protocol
        config.alpn_protocols = vec![b"mesh/1".to_vec()];

        info!("TLS server configuration created successfully");
        Ok(config)
    }

    /// Create a TLS client configuration with mTLS
    pub fn make_client_config(
        cert_chain_pem: &str,
        private_key_pem: &str,
        ca_pem: &str,
    ) -> Result<ClientConfig> {
        info!("Creating TLS client configuration with mTLS");

        // Install default crypto provider if not already set
        let _ = rustls::crypto::ring::default_provider().install_default();

        // Load CA certificates for server verification
        let mut roots = RootCertStore::empty();
        let ca_results: Result<Vec<_>, _> = rustls_pemfile::certs(&mut ca_pem.as_bytes()).collect();
        let ca_certs = ca_results.context("Failed to parse CA certificates")?;

        for ca_cert in ca_certs {
            roots
                .add(CertificateDer::from(ca_cert))
                .context("Failed to add CA certificate to root store")?;
        }

        // Load client certificate chain
        let cert_results: Result<Vec<_>, _> =
            rustls_pemfile::certs(&mut cert_chain_pem.as_bytes()).collect();
        let certs = cert_results
            .context("Failed to parse certificate chain")?
            .into_iter()
            .map(CertificateDer::from)
            .collect::<Vec<_>>();

        if certs.is_empty() {
            anyhow::bail!("No certificates found in certificate chain");
        }

        // Load private key
        let key = {
            let key_results: Result<Vec<_>, _> =
                rustls_pemfile::pkcs8_private_keys(&mut private_key_pem.as_bytes()).collect();
            let mut keys = key_results.context("Failed to parse private key")?;
            if keys.is_empty() {
                anyhow::bail!("No private key found");
            }
            PrivateKeyDer::from(keys.remove(0))
        };

        // Build client configuration
        let mut config = ClientConfig::builder()
            .with_root_certificates(roots)
            .with_client_auth_cert(certs, key)
            .context("Failed to configure client certificate")?;

        // Set ALPN protocol
        config.alpn_protocols = vec![b"mesh/1".to_vec()];

        info!("TLS client configuration created successfully");
        Ok(config)
    }

    /// Create TLS acceptor from server configuration
    pub fn tls_acceptor(config: ServerConfig) -> TlsServer {
        TlsServer {
            acceptor: TlsAcceptor::from(Arc::new(config)),
        }
    }

    /// Accept a TLS connection and return the stream with connection info
    pub async fn accept_tls(
        acceptor: &TlsServer,
        tcp_stream: TcpStream,
    ) -> Result<(IoStream, Vec<u8>)> {
        let peer_addr = tcp_stream
            .peer_addr()
            .unwrap_or_else(|_| "unknown".parse().unwrap());
        debug!("Accepting TLS connection from {}", peer_addr);

        let tls_stream = acceptor
            .acceptor
            .accept(tcp_stream)
            .await
            .with_context(|| format!("TLS handshake failed with {}", peer_addr))?;

        // Extract peer certificate
        let peer_cert = tls_stream
            .get_ref()
            .1
            .peer_certificates()
            .and_then(|certs| certs.first())
            .map(|cert| cert.as_ref().to_vec())
            .unwrap_or_default();

        debug!(
            "TLS connection accepted, peer cert length: {}",
            peer_cert.len()
        );
        Ok((IoStream::Tls(tls_stream), peer_cert))
    }

    /// Connect via TLS and return the stream with connection info
    pub async fn connect_tls(
        config: ClientConfig,
        tcp_stream: TcpStream,
        sni: &str,
    ) -> Result<(IoStream, Vec<u8>)> {
        let peer_addr = tcp_stream
            .peer_addr()
            .unwrap_or_else(|_| "unknown".parse().unwrap());
        debug!("Connecting via TLS to {} (SNI: {})", peer_addr, sni);

        let connector = TlsConnector::from(Arc::new(config));
        let server_name = ServerName::try_from(sni.to_owned())
            .map_err(|_| anyhow::anyhow!("Invalid server name: {}", sni))?;

        let tls_stream = connector
            .connect(server_name, tcp_stream)
            .await
            .with_context(|| format!("TLS handshake failed with {} (SNI: {})", peer_addr, sni))?;

        // Extract peer certificate
        let peer_cert = tls_stream
            .get_ref()
            .1
            .peer_certificates()
            .and_then(|certs| certs.first())
            .map(|cert| cert.as_ref().to_vec())
            .unwrap_or_default();

        debug!(
            "TLS connection established, peer cert length: {}",
            peer_cert.len()
        );
        Ok((IoStream::TlsClient(tls_stream), peer_cert))
    }

    /// Extract node ID from certificate SAN URI
    pub fn extract_node_id_from_cert(cert_der: &[u8]) -> Result<u64> {
        let (_remaining, cert) = x509_parser::parse_x509_certificate(cert_der)
            .map_err(|e| anyhow::anyhow!("Failed to parse X.509 certificate: {:?}", e))?;

        if let Ok(Some(san_ext)) = cert.subject_alternative_name() {
            for general_name in &san_ext.value.general_names {
                if let x509_parser::extensions::GeneralName::URI(uri) = general_name {
                    // Look for mesh://node/<u64> format
                    if let Some(node_id_str) = uri.strip_prefix("mesh://node/") {
                        let node_id: u64 = node_id_str
                            .parse()
                            .context("Failed to parse node ID from certificate URI")?;
                        debug!("Extracted node ID {} from certificate", node_id);
                        return Ok(node_id);
                    }
                }
            }
        }

        anyhow::bail!("Node ID not found in certificate SAN URI (expected mesh://node/<id>)")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr};

    #[tokio::test]
    async fn test_tcp_listen_connect() {
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
        let listener = listen_tcp(addr).await.unwrap();
        let bound_addr = listener.local_addr().unwrap();

        // Test connection
        let stream = connect_tcp(bound_addr).await.unwrap();
        let io_stream = IoStream::Plain(stream);

        // Test that we can get peer address
        assert!(io_stream.peer_addr().is_ok());
    }

    #[cfg(feature = "tls")]
    #[test]
    fn test_node_id_extraction() {
        // This would require a real certificate for a full test
        // For now, just test that the function exists and handles errors gracefully
        let result = tls::extract_node_id_from_cert(&[]);
        assert!(result.is_err());
    }
}
