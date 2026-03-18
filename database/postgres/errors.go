//nolint:gochecknoglobals // Package-level matchers are the intended API.
package postgres

import (
	"errors"
	"net"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"
)

// IsTransientError reports whether err is a transient PostgreSQL error that
// may succeed on retry (closed connections, connection exceptions, or
// serialization failures).
func IsTransientError(err error) bool {
	return errors.Is(err, net.ErrClosed) || IsConnectionException(err) || IsSerializationFailure(err)
}

// IsConnectionException reports whether err is a PostgreSQL class 08
// (connection exception) error.
var IsConnectionException = pgErrMatcher(func(err *pgconn.PgError) bool {
	return strings.HasPrefix(err.Code, "08")
})

// IsRollback reports whether err is a PostgreSQL 40000 (transaction rollback) error.
var IsRollback = pgErrMatcher(func(err *pgconn.PgError) bool {
	return err.Code == "40000"
})

// IsSerializationFailure reports whether err is a PostgreSQL 40001
// (serialization failure) error.
var IsSerializationFailure = pgErrMatcher(func(err *pgconn.PgError) bool {
	return err.Code == "40001"
})

// IsUniqueConstraintViolation reports whether err is a unique constraint
// violation, either at statement execution time (23505) or at transaction
// commit time (40002).
var IsUniqueConstraintViolation = pgErrMatcher(func(err *pgconn.PgError) bool {
	return err.Code == "40002" || err.Code == "23505"
})

// IsPGErrorFunc reports whether err is a PostgreSQL error matching the given
// predicate. It can be used for ad-hoc error checks that aren't covered by the
// pre-defined matchers.
func IsPGErrorFunc(err error, predicate func(*pgconn.PgError) bool) bool {
	return pgErrMatcher(predicate)(err)
}

func pgErrMatcher(predicate func(pgError *pgconn.PgError) bool) func(error) bool {
	return func(err error) bool {
		if err, ok := errors.AsType[*pgconn.PgError](err); ok {
			return predicate(err)
		}
		return false
	}
}
