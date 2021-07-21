package pgadvisorylock

import (
	"context"
	"database/sql"
	"encoding/json"

	"github.com/zeebo/xxh3"
)

type AdvisoryLock struct {
	Pid      int64  `json:"pid"`      // the process id of the process that acquired the lock
	ObjectID int64  `json:"objectID"` // ObjectID when using 32 bit lock with class id
	ClassID  int64  `json:"classID"`  // ClassID when using 32 bit lock with object id
	Granted  bool   `json:"granted"`  // Whether the lock is held or not
	Locktype string `json:"locktype"` // the type of lock
}

// AcquireLock acquires a session-level postgresql advisory lock
// uses pg_try_advisory_lock which returns immediately
func AcquireLockInt64(p *sql.DB, ctx context.Context, lockID int64) (bool, error) {
	var isLockAquired bool = false

	err := p.QueryRowContext(ctx, "SELECT pg_try_advisory_lock($1);", lockID).Scan(&isLockAquired)
	if err != nil {
		return false, err
	}
	return isLockAquired, nil
}

// AcquireLock acquires a shared session-level postgresql advisory lock
// uses pg_try_advisory_lock which returns immediately
func AcquireSharedLockInt64(p *sql.DB, ctx context.Context, lockID int64) (bool, error) {
	var isLockAquired bool = false

	err := p.QueryRowContext(ctx, "SELECT pg_try_advisory_lock_shared($1);", lockID).Scan(&isLockAquired)
	if err != nil {
		return false, err
	}
	return isLockAquired, nil
}

// AcquireLock acquires a session-level postgresql advisory lock
// uses pg_try_advisory_lock which returns immediately
func AcquireSharedLock(p *sql.DB, ctx context.Context, lockID string) (bool, int64, error) {
	lockIDHash := int64(xxh3.HashString(lockID))
	ok, err := AcquireSharedLockInt64(p, ctx, lockIDHash)
	if err != nil {
		return false, 0, err
	}

	return ok, lockIDHash, nil
}

// AcquireLock acquires a transaction-level postgresql advisory lock
// uses pg_try_advisory_xact_lock which returns immediately
func AcquireTxnLock(p *sql.Tx, ctx context.Context, lockID int64) (bool, error) {
	var isLockAquired bool = false
	// fmt.Println("Acquiring lock on id:", lockID)
	err := p.QueryRowContext(ctx, "SELECT pg_try_advisory_xact_lock($1);", lockID).Scan(&isLockAquired)
	if err != nil {
		return false, err
	}
	return isLockAquired, nil
}

// AcquireLock acquires a session-level postgresql advisory lock
// Hashes the value with xxh3 hash to generate a unique lockID
// see: AcquireLock
func AcquireLock(p *sql.DB, ctx context.Context, lockID string) (bool, int64, error) {
	lockIDHash := int64(xxh3.HashString(lockID))
	ok, err := AcquireLockInt64(p, ctx, lockIDHash)
	if err != nil {
		return false, 0, err
	}

	return ok, lockIDHash, nil
}

// ReleaseLock releases an advisory lock and returns whether lock was released
// successfully or not
func ReleaseLock(p *sql.DB, ctx context.Context, lockID int64) (bool, error) {
	var isLockReleased bool
	err := p.QueryRowContext(ctx, "SELECT pg_advisory_unlock($1)", lockID).Scan(&isLockReleased)
	if err != nil {
		return false, err
	}

	return isLockReleased, nil
}

// ReleaseLock releases a shared session-level advisory lock
// and returns whether lock was released successfully or not
func ReleaseSharedLock(p *sql.DB, ctx context.Context, lockID int64) (bool, error) {
	var isLockReleased bool
	err := p.QueryRowContext(ctx, "SELECT pg_advisory_unlock_shared($1::bigint)", lockID).Scan(&isLockReleased)
	if err != nil {
		return false, err
	}

	return isLockReleased, nil
}

func FetchAdvisoryLocks(conn *sql.DB, ctx context.Context) ([]*AdvisoryLock, error) {
	rows, err := conn.QueryContext(ctx, "SELECT json_build_object('objectID', objid::integer, 'classID', classid, 'pid', pid, 'granted', granted, 'locktype', locktype) FROM pg_locks WHERE locktype = 'advisory'")

	advisoryLocks := make([]*AdvisoryLock, 0)
	defer rows.Close()

	for rows.Next() {
		var jsonstring string
		err = rows.Scan(&jsonstring)
		if err != nil {
			return nil, err
		}

		lock := new(AdvisoryLock)
		json.Unmarshal([]byte(jsonstring), &lock)

		advisoryLocks = append(advisoryLocks, lock)
	}

	return advisoryLocks, nil
}
