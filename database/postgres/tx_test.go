package postgres

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"testing/synctest"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestTryTxFunc_Success(t *testing.T) {
	db := &mockDB{beginTxFn: func() (pgx.Tx, error) {
		return &mockTx{}, nil
	}}

	var called bool
	err := TryTxFunc(context.Background(), db, pgx.TxOptions{}, func(tx pgx.Tx) error {
		called = true
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !called {
		t.Fatal("fn was not called")
	}
}

func TestTryTxFunc_NonTransientError(t *testing.T) {
	db := &mockDB{beginTxFn: func() (pgx.Tx, error) {
		return &mockTx{}, nil
	}}

	want := errors.New("permanent failure")
	err := TryTxFunc(context.Background(), db, pgx.TxOptions{}, func(tx pgx.Tx) error {
		return want
	})
	if !errors.Is(err, want) {
		t.Fatalf("got %v, want %v", err, want)
	}
}

func TestTryTxFunc_RetriesThenSucceeds(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var attempts atomic.Int32

		serializationErr := &pgconn.PgError{Code: "40001"}

		db := &mockDB{beginTxFn: func() (pgx.Tx, error) {
			return &mockTx{}, nil
		}}

		err := TryTxFunc(context.Background(), db, pgx.TxOptions{}, func(tx pgx.Tx) error {
			if attempts.Add(1) <= 2 {
				return serializationErr
			}
			return nil
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got := attempts.Load(); got != 3 {
			t.Fatalf("expected 3 attempts, got %d", got)
		}
	})
}

func TestTryTxFunc_GivesUpAfterMaxRetries(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		serializationErr := &pgconn.PgError{Code: "40001"}

		db := &mockDB{beginTxFn: func() (pgx.Tx, error) {
			return &mockTx{}, nil
		}}

		err := TryTxFunc(context.Background(), db, pgx.TxOptions{}, func(tx pgx.Tx) error {
			return serializationErr
		})
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if !errors.Is(err, serializationErr) {
			t.Fatalf("expected serialization error in chain, got: %v", err)
		}
	})
}

func TestTryTxFunc_RespectsContextCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		serializationErr := &pgconn.PgError{Code: "40001"}

		db := &mockDB{beginTxFn: func() (pgx.Tx, error) {
			return &mockTx{}, nil
		}}

		ctx, cancel := context.WithCancel(context.Background())

		var attempts atomic.Int32
		err := TryTxFunc(ctx, db, pgx.TxOptions{}, func(tx pgx.Tx) error {
			if attempts.Add(1) == 2 {
				cancel()
			}
			return serializationErr
		})
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled, got: %v", err)
		}
	})
}

// mockDB implements the BeginTx interface accepted by TryTxFunc.
type mockDB struct {
	beginTxFn func() (pgx.Tx, error)
}

func (m *mockDB) BeginTx(
	ctx context.Context,
	_ pgx.TxOptions, //nolint:gocritic // must match interface
) (pgx.Tx, error) {
	return m.beginTxFn()
}

// mockTx implements pgx.Tx with just enough to satisfy pgx.BeginTxFunc.
type mockTx struct {
	commitErr error
}

func (m *mockTx) Commit(ctx context.Context) error   { return m.commitErr }
func (m *mockTx) Rollback(ctx context.Context) error { return nil }
func (m *mockTx) Begin(ctx context.Context) (pgx.Tx, error) {
	return &mockTx{}, nil
}

func (m *mockTx) CopyFrom(
	ctx context.Context,
	tableName pgx.Identifier,
	columnNames []string,
	rowSrc pgx.CopyFromSource,
) (int64, error) {
	panic("not implemented")
}

func (m *mockTx) SendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults {
	panic("not implemented")
}
func (m *mockTx) LargeObjects() pgx.LargeObjects { panic("not implemented") }
func (m *mockTx) Prepare(ctx context.Context, name, sql string) (*pgconn.StatementDescription, error) {
	panic("not implemented")
}

func (m *mockTx) Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error) {
	panic("not implemented")
}

func (m *mockTx) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	panic("not implemented")
}

func (m *mockTx) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	panic("not implemented")
}
func (m *mockTx) Conn() *pgx.Conn { panic("not implemented") }
