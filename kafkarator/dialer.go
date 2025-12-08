package kafkarator

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

func dialer(c Config) (*kafka.Dialer, error) {
	d := &kafka.Dialer{
		Timeout:   10 * time.Second,
		DualStack: true,
	}

	// If TLS certificates are provided, configure TLS
	if c.CertFile != "" || c.KeyFile != "" || c.CAFile != "" {
		keypair, err := tls.LoadX509KeyPair(c.CertFile, c.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("load key pair: %w", err)
		}

		caCert, err := os.ReadFile(c.CAFile)
		if err != nil {
			return nil, fmt.Errorf("read ca cert: %w", err)
		}

		// Start with the system's root CAs (for server certificate validation)
		caCertPool, err := x509.SystemCertPool()
		if err != nil {
			// If we can't get system certs, create a new pool
			caCertPool = x509.NewCertPool()
		}

		// Append the project CA (for client certificate validation in mTLS)
		ok := caCertPool.AppendCertsFromPEM(caCert)
		if !ok {
			return nil, fmt.Errorf("failed to parse CA certificate file")
		}

		// Extract hostname from the first broker for ServerName in TLS config
		// This is critical for certificate validation
		serverName := extractHostname(c.Brokers[0])

		d.TLS = &tls.Config{
			Certificates:       []tls.Certificate{keypair},
			RootCAs:            caCertPool,
			ServerName:         serverName, // Required for proper certificate validation
			MinVersion:         tls.VersionTLS12,
			InsecureSkipVerify: false, // Keep verification enabled
		}
	}

	return d, nil
}

// extractHostname extracts the hostname from a broker address (removes port)
func extractHostname(broker string) string {
	// broker format is typically "hostname:port"
	if idx := strings.LastIndex(broker, ":"); idx != -1 {
		return broker[:idx]
	}
	return broker
}

func testConnection(ctx context.Context, brokers []string, dialer *kafka.Dialer) error {
	if dialer == nil {
		return fmt.Errorf("dialer is nil")
	}
	if len(brokers) == 0 {
		return fmt.Errorf("no brokers configured")
	}

	// Try to connect to the first broker
	conn, err := dialer.DialContext(ctx, "tcp", brokers[0])
	if err != nil {
		return fmt.Errorf("failed to connect to broker %s: %w", brokers[0], err)
	}
	defer conn.Close()

	// Try to get broker metadata to verify the connection works
	_, err = conn.Brokers()
	if err != nil {
		return fmt.Errorf("failed to retrieve broker metadata: %w", err)
	}

	return nil
}
