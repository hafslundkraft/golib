package telemetry

import (
	"context"
	"log/slog"
)

type loggerContextKey struct{}

// NewContextWithLogger returns a new context carrying the given slog.Logger.
func NewContextWithLogger(ctx context.Context, logger *slog.Logger) context.Context {
	return context.WithValue(ctx, loggerContextKey{}, logger)
}

// LoggerFromContext retrieves the slog.Logger stored by NewContextWithLogger.
// Falls back to slog.Default() when none is present.
func LoggerFromContext(ctx context.Context) *slog.Logger {
	if l, ok := ctx.Value(loggerContextKey{}).(*slog.Logger); ok {
		return l
	}
	return slog.Default()
}
