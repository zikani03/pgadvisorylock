package pgx

import (
	"context"
	"database/sql"

	pgx "github.com/jackc/pgx/v5"
	"github.com/zeebo/xxh3"
)

// AcquireLock acquires a session-level postgresql advisory lock
// uses pg_try_advisory_lock which returns immediately
func AcquireLockInt64(p *pgx.Conn, ctx context.Context, lockID int64) (bool, error) {
	var isLockAquired bool = false

	err := p.QueryRow(ctx, "SELECT pg_try_advisory_lock($1);", lockID).Scan(&isLockAquired)
	if err != nil {
		return false, err
	}
	return isLockAquired, nil
}

// AcquireLock acquires a shared session-level postgresql advisory lock
// uses pg_try_advisory_lock which returns immediately
func AcquireSharedLockInt64(p *pgx.Conn, ctx context.Context, lockID int64) (bool, error) {
	var isLockAquired bool = false

	err := p.QueryRow(ctx, "SELECT pg_try_advisory_lock_shared($1);", lockID).Scan(&isLockAquired)
	if err != nil {
		return false, err
	}
	return isLockAquired, nil
}

// AcquireLock acquires a session-level postgresql advisory lock
// uses pg_try_advisory_lock which returns immediately
func AcquireSharedLock(p *pgx.Conn, ctx context.Context, lockID string) (bool, int64, error) {
	lockIDHash := int64(xxh3.HashString(lockID))
	ok, err := AcquireSharedLockInt64(p, ctx, lockIDHash)
	if err != nil {
		return false, 0, err
	}

	return ok, lockIDHash, nil
}

// AcquireLock acquires a transaction-level postgresql advisory lock
// uses pg_try_advisory_xact_lock which returns immediately
func AcquireTxnLock(p *sql.Tx, lockID int64) (bool, error) {
	var isLockAquired bool = false
	// fmt.Println("Acquiring lock on id:", lockID)
	err := p.QueryRow("SELECT pg_try_advisory_xact_lock($1);", lockID).Scan(&isLockAquired)
	if err != nil {
		return false, err
	}
	return isLockAquired, nil
}

// AcquireLock acquires a session-level postgresql advisory lock
// Hashes the value with xxh3 hash to generate a unique lockID
// see: AcquireLock
func AcquireLock(p *pgx.Conn, ctx context.Context, lockID string) (bool, int64, error) {
	lockIDHash := int64(xxh3.HashString(lockID))
	ok, err := AcquireLockInt64(p, ctx, lockIDHash)
	if err != nil {
		return false, 0, err
	}

	return ok, lockIDHash, nil
}

// ReleaseLock releases an advisory lock and returns whether lock was released
// successfully or not
func ReleaseLock(p *pgx.Conn, ctx context.Context, lockID int64) (bool, error) {
	var isLockReleased bool
	err := p.QueryRow(ctx, "SELECT pg_advisory_unlock($1)", lockID).Scan(&isLockReleased)
	if err != nil {
		return false, err
	}

	return isLockReleased, nil
}

// ReleaseLock releases a shared session-level advisory lock
// and returns whether lock was released successfully or not
func ReleaseSharedLock(p *pgx.Conn, ctx context.Context, lockID int64) (bool, error) {
	var isLockReleased bool
	err := p.QueryRow(ctx, "SELECT pg_advisory_unlock_shared($1::bigint)", lockID).Scan(&isLockReleased)
	if err != nil {
		return false, err
	}

	return isLockReleased, nil
}
