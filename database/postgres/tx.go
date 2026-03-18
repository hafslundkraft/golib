package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// TryTxFunc executes fn within a transaction, retrying on transient errors
// (connection exceptions, serialization failures) with exponential backoff
// starting at 100ms. It gives up after approximately 13 seconds of retrying.
func TryTxFunc(
	ctx context.Context,
	db interface {
		BeginTx(ctx context.Context, txOptions pgx.TxOptions) (pgx.Tx, error)
	},
	txOptions pgx.TxOptions, //nolint:gocritic // heavy struct, but same as pgx.BeginTxFunc
	fn func(pgx.Tx) error,
) error {
	backoff := 100 * time.Millisecond
	for {
		if err := pgx.BeginTxFunc(ctx, db, txOptions, fn); !IsTransientError(err) {
			if err != nil {
				return fmt.Errorf("transaction: %w", err)
			}
			return nil
		} else if backoff > 8*time.Second {
			return fmt.Errorf("after retrying: %w", err)
		}

		select {
		case <-time.After(backoff):
			backoff *= 2
		case <-ctx.Done():
			return fmt.Errorf("waiting for retry: %w", ctx.Err())
		}
	}
}
