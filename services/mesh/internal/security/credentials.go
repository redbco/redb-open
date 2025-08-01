package security

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"time"

	"github.com/redbco/redb-open/pkg/logger"
	"github.com/redbco/redb-open/services/mesh/internal/storage"
)

// MeshCredentials represents the cryptographic credentials for a mesh node
type MeshCredentials struct {
	NodeID      string
	MeshID      string
	Certificate *x509.Certificate
	PrivateKey  *rsa.PrivateKey
	CACert      *x509.Certificate
	TLSConfig   *tls.Config
}

// CredentialManager manages mesh node credentials and security
type CredentialManager struct {
	storage storage.Interface
	logger  *logger.Logger
}

// NewCredentialManager creates a new credential manager
func NewCredentialManager(storage storage.Interface, logger *logger.Logger) *CredentialManager {
	return &CredentialManager{
		storage: storage,
		logger:  logger,
	}
}

// GenerateMeshCredentials generates new credentials for seeding a mesh
func (cm *CredentialManager) GenerateMeshCredentials(ctx context.Context, meshID, nodeID string) (*MeshCredentials, error) {
	cm.logger.Infof("Generating mesh credentials for node %s in mesh %s", nodeID, meshID)

	// Generate CA certificate for the mesh
	caCert, caKey, err := cm.generateCACertificate(meshID)
	if err != nil {
		return nil, fmt.Errorf("failed to generate CA certificate: %v", err)
	}

	// Generate node certificate
	nodeCert, nodeKey, err := cm.generateNodeCertificate(nodeID, meshID, caCert, caKey)
	if err != nil {
		return nil, fmt.Errorf("failed to generate node certificate: %v", err)
	}

	// Create TLS configuration
	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{
			{
				Certificate: [][]byte{nodeCert.Raw, caCert.Raw},
				PrivateKey:  nodeKey,
			},
		},
		RootCAs:    x509.NewCertPool(),
		ClientCAs:  x509.NewCertPool(),
		ClientAuth: tls.RequireAndVerifyClientCert,
		MinVersion: tls.VersionTLS12,
	}
	tlsConfig.RootCAs.AddCert(caCert)
	tlsConfig.ClientCAs.AddCert(caCert)

	credentials := &MeshCredentials{
		NodeID:      nodeID,
		MeshID:      meshID,
		Certificate: nodeCert,
		PrivateKey:  nodeKey,
		CACert:      caCert,
		TLSConfig:   tlsConfig,
	}

	// Store credentials
	if err := cm.storeCredentials(ctx, credentials); err != nil {
		return nil, fmt.Errorf("failed to store credentials: %v", err)
	}

	cm.logger.Infof("Generated and stored mesh credentials for node %s", nodeID)
	return credentials, nil
}

// LoadMeshCredentials loads existing credentials from storage
func (cm *CredentialManager) LoadMeshCredentials(meshID, nodeID string) (*MeshCredentials, error) {
	cm.logger.Infof("Loading mesh credentials for node %s in mesh %s", nodeID, meshID)

	// Load from storage
	certData, err := cm.storage.GetConfig(nil, fmt.Sprintf("mesh.%s.node.%s.certificate", meshID, nodeID))
	if err != nil {
		return nil, fmt.Errorf("failed to load certificate: %v", err)
	}

	keyData, err := cm.storage.GetConfig(nil, fmt.Sprintf("mesh.%s.node.%s.private_key", meshID, nodeID))
	if err != nil {
		return nil, fmt.Errorf("failed to load private key: %v", err)
	}

	caData, err := cm.storage.GetConfig(nil, fmt.Sprintf("mesh.%s.ca_certificate", meshID))
	if err != nil {
		return nil, fmt.Errorf("failed to load CA certificate: %v", err)
	}

	// Parse certificates and keys
	cert, err := cm.parseCertificate(certData.(string))
	if err != nil {
		return nil, fmt.Errorf("failed to parse certificate: %v", err)
	}

	key, err := cm.parsePrivateKey(keyData.(string))
	if err != nil {
		return nil, fmt.Errorf("failed to parse private key: %v", err)
	}

	caCert, err := cm.parseCertificate(caData.(string))
	if err != nil {
		return nil, fmt.Errorf("failed to parse CA certificate: %v", err)
	}

	// Create TLS configuration
	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{
			{
				Certificate: [][]byte{cert.Raw, caCert.Raw},
				PrivateKey:  key,
			},
		},
		RootCAs:    x509.NewCertPool(),
		ClientCAs:  x509.NewCertPool(),
		ClientAuth: tls.RequireAndVerifyClientCert,
		MinVersion: tls.VersionTLS12,
	}
	tlsConfig.RootCAs.AddCert(caCert)
	tlsConfig.ClientCAs.AddCert(caCert)

	credentials := &MeshCredentials{
		NodeID:      nodeID,
		MeshID:      meshID,
		Certificate: cert,
		PrivateKey:  key,
		CACert:      caCert,
		TLSConfig:   tlsConfig,
	}

	cm.logger.Infof("Loaded mesh credentials for node %s", nodeID)
	return credentials, nil
}

// GenerateJoinCredentials generates credentials for a node joining an existing mesh
func (cm *CredentialManager) GenerateJoinCredentials(ctx context.Context, meshID, nodeID, meshToken string) (*MeshCredentials, error) {
	cm.logger.Infof("Generating join credentials for node %s in mesh %s", nodeID, meshID)

	// Load CA certificate from mesh token (simplified - in production this would involve secure token exchange)
	caData, err := cm.storage.GetConfig(ctx, fmt.Sprintf("mesh.%s.ca_certificate", meshID))
	if err != nil {
		return nil, fmt.Errorf("failed to load CA certificate: %v", err)
	}

	caCert, err := cm.parseCertificate(caData.(string))
	if err != nil {
		return nil, fmt.Errorf("failed to parse CA certificate: %v", err)
	}

	// For now, we'll generate a self-signed certificate
	// In production, this would involve a CSR to the mesh CA
	// Generate new node certificate with CA
	nodeCert, nodeKey, err := cm.generateNodeCertificate(nodeID, meshID, caCert, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to generate node certificate: %v", err)
	}

	// Create TLS configuration
	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{
			{
				Certificate: [][]byte{nodeCert.Raw, caCert.Raw},
				PrivateKey:  nodeKey,
			},
		},
		RootCAs:    x509.NewCertPool(),
		ClientCAs:  x509.NewCertPool(),
		ClientAuth: tls.RequireAndVerifyClientCert,
		MinVersion: tls.VersionTLS12,
	}
	tlsConfig.RootCAs.AddCert(caCert)
	tlsConfig.ClientCAs.AddCert(caCert)

	credentials := &MeshCredentials{
		NodeID:      nodeID,
		MeshID:      meshID,
		Certificate: nodeCert,
		PrivateKey:  nodeKey,
		CACert:      caCert,
		TLSConfig:   tlsConfig,
	}

	// Store credentials
	if err := cm.storeCredentials(ctx, credentials); err != nil {
		return nil, fmt.Errorf("failed to store credentials: %v", err)
	}

	cm.logger.Infof("Generated and stored join credentials for node %s", nodeID)
	return credentials, nil
}

// generateCACertificate generates a CA certificate for the mesh
func (cm *CredentialManager) generateCACertificate(meshID string) (*x509.Certificate, *rsa.PrivateKey, error) {
	// Generate private key
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to generate private key: %v", err)
	}

	// Create certificate template
	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			Organization:  []string{"reDB Mesh"},
			Country:       []string{"US"},
			Province:      []string{""},
			Locality:      []string{""},
			StreetAddress: []string{""},
			PostalCode:    []string{""},
			CommonName:    fmt.Sprintf("reDB Mesh CA %s", meshID),
		},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(365 * 24 * time.Hour), // 1 year
		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
	}

	// Generate certificate
	certDER, err := x509.CreateCertificate(rand.Reader, &template, &template, &key.PublicKey, key)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create certificate: %v", err)
	}

	// Parse certificate
	cert, err := x509.ParseCertificate(certDER)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to parse certificate: %v", err)
	}

	return cert, key, nil
}

// generateNodeCertificate generates a node certificate signed by the CA
func (cm *CredentialManager) generateNodeCertificate(nodeID, meshID string, caCert *x509.Certificate, caKey *rsa.PrivateKey) (*x509.Certificate, *rsa.PrivateKey, error) {
	// Generate private key
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to generate private key: %v", err)
	}

	// Create certificate template
	template := x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject: pkix.Name{
			Organization:  []string{"reDB Mesh"},
			Country:       []string{"US"},
			Province:      []string{""},
			Locality:      []string{""},
			StreetAddress: []string{""},
			PostalCode:    []string{""},
			CommonName:    fmt.Sprintf("reDB Node %s", nodeID),
		},
		NotBefore:   time.Now(),
		NotAfter:    time.Now().Add(365 * 24 * time.Hour), // 1 year
		KeyUsage:    x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		DNSNames:    []string{nodeID, fmt.Sprintf("%s.%s", nodeID, meshID)},
		IPAddresses: []net.IP{},
	}

	// If no CA key provided, create self-signed certificate
	signerCert := caCert
	signerKey := caKey
	if caKey == nil {
		signerCert = &template
		signerKey = key
	}

	// Generate certificate
	certDER, err := x509.CreateCertificate(rand.Reader, &template, signerCert, &key.PublicKey, signerKey)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create certificate: %v", err)
	}

	// Parse certificate
	cert, err := x509.ParseCertificate(certDER)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to parse certificate: %v", err)
	}

	return cert, key, nil
}

// storeCredentials stores credentials in the storage backend
func (cm *CredentialManager) storeCredentials(ctx context.Context, creds *MeshCredentials) error {
	// Store certificate
	certPEM := cm.encodeCertificate(creds.Certificate)
	if err := cm.storage.SaveConfig(ctx, fmt.Sprintf("mesh.%s.node.%s.certificate", creds.MeshID, creds.NodeID), certPEM); err != nil {
		return fmt.Errorf("failed to store certificate: %v", err)
	}

	// Store private key
	keyPEM := cm.encodePrivateKey(creds.PrivateKey)
	if err := cm.storage.SaveConfig(ctx, fmt.Sprintf("mesh.%s.node.%s.private_key", creds.MeshID, creds.NodeID), keyPEM); err != nil {
		return fmt.Errorf("failed to store private key: %v", err)
	}

	// Store CA certificate
	caCertPEM := cm.encodeCertificate(creds.CACert)
	if err := cm.storage.SaveConfig(ctx, fmt.Sprintf("mesh.%s.ca_certificate", creds.MeshID), caCertPEM); err != nil {
		return fmt.Errorf("failed to store CA certificate: %v", err)
	}

	return nil
}

// encodeCertificate encodes a certificate to PEM format
func (cm *CredentialManager) encodeCertificate(cert *x509.Certificate) string {
	certPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "CERTIFICATE",
		Bytes: cert.Raw,
	})
	return string(certPEM)
}

// encodePrivateKey encodes a private key to PEM format
func (cm *CredentialManager) encodePrivateKey(key *rsa.PrivateKey) string {
	keyPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(key),
	})
	return string(keyPEM)
}

// parseCertificate parses a PEM-encoded certificate
func (cm *CredentialManager) parseCertificate(certPEM string) (*x509.Certificate, error) {
	block, _ := pem.Decode([]byte(certPEM))
	if block == nil {
		return nil, fmt.Errorf("failed to decode PEM block")
	}

	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("failed to parse certificate: %v", err)
	}

	return cert, nil
}

// parsePrivateKey parses a PEM-encoded private key
func (cm *CredentialManager) parsePrivateKey(keyPEM string) (*rsa.PrivateKey, error) {
	block, _ := pem.Decode([]byte(keyPEM))
	if block == nil {
		return nil, fmt.Errorf("failed to decode PEM block")
	}

	key, err := x509.ParsePKCS1PrivateKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("failed to parse private key: %v", err)
	}

	return key, nil
}

// ValidatePeerCertificate validates a peer certificate against the mesh CA
func (cm *CredentialManager) ValidatePeerCertificate(cert *x509.Certificate, meshID string) error {
	// Load CA certificate
	caData, err := cm.storage.GetConfig(nil, fmt.Sprintf("mesh.%s.ca_certificate", meshID))
	if err != nil {
		return fmt.Errorf("failed to load CA certificate: %v", err)
	}

	caCert, err := cm.parseCertificate(caData.(string))
	if err != nil {
		return fmt.Errorf("failed to parse CA certificate: %v", err)
	}

	// Create cert pool with CA
	roots := x509.NewCertPool()
	roots.AddCert(caCert)

	// Verify certificate
	opts := x509.VerifyOptions{
		Roots: roots,
	}

	_, err = cert.Verify(opts)
	if err != nil {
		return fmt.Errorf("certificate verification failed: %v", err)
	}

	return nil
}

// GetMeshToken generates a token for joining the mesh (simplified implementation)
func (cm *CredentialManager) GetMeshToken(meshID string) (string, error) {
	// In production, this would generate a secure, time-limited token
	// For now, return a simple token
	return fmt.Sprintf("mesh-token-%s-%d", meshID, time.Now().Unix()), nil
}

// ValidateMeshToken validates a mesh join token
func (cm *CredentialManager) ValidateMeshToken(token, meshID string) error {
	// In production, this would verify the token's signature and expiration
	// For now, just check if it contains the mesh ID
	if fmt.Sprintf("mesh-token-%s", meshID) == token[:len(token)-11] {
		return nil
	}
	return fmt.Errorf("invalid mesh token")
}
