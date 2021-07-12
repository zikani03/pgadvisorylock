package pgadvisorylock

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/zeebo/xxh3"
)

// AcquireLock acquires a session-level postgresql advisory lock
// uses pg_try_advisory_lock which returns immediately
func AcquireLockInt64(p *sql.DB, lockID int64) (bool, error) {
	var isLockAquired bool = false
	// fmt.Println("Acquiring lock on id:", lockID)
	err := p.QueryRowContext(context.Background(), "SELECT pg_try_advisory_lock($1);", lockID).Scan(&isLockAquired)
	if err != nil {
		return false, err
	}
	return isLockAquired, nil
}

// AcquireLock acquires a shared session-level postgresql advisory lock
// uses pg_try_advisory_lock which returns immediately
func AcquireSharedLockInt64(p *sql.DB, lockID int64) (bool, error) {
	var isLockAquired bool = false
	// fmt.Println("Acquiring lock on id:", lockID)
	err := p.QueryRowContext(context.Background(), "SELECT pg_try_advisory_lock_shared($1);", lockID).Scan(&isLockAquired)
	if err != nil {
		return false, err
	}
	return isLockAquired, nil
}

// AcquireLock acquires a session-level postgresql advisory lock
// uses pg_try_advisory_lock which returns immediately
func AcquireSharedLock(p *sql.DB, lockID string) (bool, int64, error) {
	lockIDHash := int64(xxh3.HashString(lockID))
	ok, err := AcquireSharedLockInt64(p, lockIDHash)
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
	err := p.QueryRowContext(context.Background(), "SELECT pg_try_advisory_xact_lock($1);", lockID).Scan(&isLockAquired)
	if err != nil {
		return false, err
	}
	return isLockAquired, nil
}

// AcquireLock acquires a session-level postgresql advisory lock
// Hashes the value with xxh3 hash to generate a unique lockID
// see: AcquireLock
func AcquireLock(p *sql.DB, lockID string) (bool, int64, error) {
	lockIDHash := int64(xxh3.HashString(lockID))
	ok, err := AcquireLockInt64(p, lockIDHash)
	if err != nil {
		return false, 0, err
	}

	return ok, lockIDHash, nil
}

// ReleaseLock releases an advisory lock and returns whether lock was released
// successfully or not
func ReleaseLock(p *sql.DB, lockID int64) (bool, error) {
	var isLockReleased bool
	err := p.QueryRowContext(context.Background(), "SELECT pg_advisory_unlock($1::bigint)", lockID).Scan(&isLockReleased)
	if err != nil {
		return false, err
	}

	return isLockReleased, nil
}

func FetchAdvisoryLocks(conn *sql.DB) error {
	rows, err := conn.QueryContext(context.Background(), "SELECT objid, pid, granted FROM pg_locks WHERE locktype='advisory'")

	defer rows.Close()
	for rows.Next() {
		var objid int64
		var pid int64
		var granted bool
		rows.Scan(&objid, &pid, &granted)

		fmt.Printf("ObjectID: %d PID:%d Granted:%v\n", objid, pid, granted)
	}

	return err
}
