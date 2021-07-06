package pgx

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v4"
	"github.com/zeebo/xxh3"
)

// AcquireLock acquires a session-level postgresql advisory lock
// uses pg_try_advisory_lock which returns immediately
func AcquireLock(p *pgx.Conn, lockID int64) (bool, error) {
	var isLockAquired bool = false
	// fmt.Println("Acquiring lock on id:", lockID)
	err := p.QueryRow(context.Background(), "SELECT pg_try_advisory_lock($1);", lockID).Scan(&isLockAquired)
	if err != nil {
		return false, err
	}
	return isLockAquired, nil
}

// AcquireLockStr acquires a session-level postgresql advisory lock
// Hashes the value with xxh3 hash to generate a unique lockID
// see: AcquireLock
func AcquireLockStr(p *pgx.Conn, val string) (bool, int64, error) {
	valxxh := xxh3.HashString(val)
	ok, err := AcquireLock(p, int64(valxxh))
	if err != nil {
		return false, 0, err
	}

	return ok, int64(valxxh), nil
}

// ReleaseLock releases an advisory lock and returns whether lock was released
// successfully or not
func ReleaseLock(p *pgx.Conn, lockID int64) (bool, error) {
	var isLockReleased bool
	err := p.QueryRow(context.Background(), "SELECT pg_advisory_unlock($1::bigint)", lockID).Scan(&isLockReleased)
	if err != nil {
		return false, err
	}

	return isLockReleased, nil
}

func FetchAdvisoryLocks(conn *pgx.Conn) error {
	rows, err := conn.Query(context.Background(), "SELECT objid, pid, granted FROM pg_locks WHERE locktype='advisory'")

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
