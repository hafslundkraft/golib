package postgres

import (
	"errors"
	"fmt"
	"net"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

func TestIsTransientError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"closed connection", fmt.Errorf("query: %w", net.ErrClosed), true},
		{"connection refused", connectError(t), true},
		{"connection exception", &pgconn.PgError{Code: "08006"}, true},
		{"admin shutdown", &pgconn.PgError{Code: "57P01"}, true},
		{"cannot connect now", &pgconn.PgError{Code: "57P03"}, true},
		{"serialization failure", &pgconn.PgError{Code: "40001"}, true},
		{"unique violation", &pgconn.PgError{Code: "23505"}, false},
		{"other error", errors.New("boom"), false},
		{"nil", nil, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsTransientError(tt.err); got != tt.want {
				t.Fatalf("IsTransientError(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}

// connectError returns the error pgconn gives for a refused connection, as
// when the read-write service has no primary behind it during a switchover.
func connectError(t *testing.T) error {
	t.Helper()
	l, err := new(net.ListenConfig).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	_ = l.Close()

	conn, err := pgconn.Connect(t.Context(), "postgres://u@"+addr+"/db?connect_timeout=1")
	if err == nil {
		_ = conn.Close(t.Context())
		t.Fatal("expected connection to fail")
	}
	if _, ok := errors.AsType[*pgconn.ConnectError](err); !ok {
		t.Fatalf("expected *pgconn.ConnectError, got %T: %v", err, err)
	}
	return fmt.Errorf("connect: %w", err)
}
