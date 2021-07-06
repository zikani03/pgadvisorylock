package pgadvisorylock_test

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v4"
	"github.com/rubenv/pgtest"
	"github.com/stretchr/testify/assert"
	pgadvisorylock "github.com/zikani03/pgadvisorylock"
	pgxadvisorylock "github.com/zikani03/pgadvisorylock/pgx"
)

func TestAdvisoryLock(t *testing.T) {
	assert := assert.New(t)

	conn, err := pgtest.Start()
	defer conn.Stop()
	assert.NoError(err)
	//var lockID int64
	acquired, lockID, err := pgadvisorylock.AcquireLockStr(conn.DB, "person:100")
	assert.True(acquired)
	assert.NoError(err)

	released, err := pgadvisorylock.ReleaseLock(conn.DB, lockID)
	assert.True(released)
	assert.NoError(err)
}

func TestPgxAdvisoryLock(t *testing.T) {
	assert := assert.New(t)

	conn, err := pgx.Connect(context.Background(), "dbname=postgres")
	defer conn.Close(context.Background())

	assert.NoError(err)
	//var lockID int64
	acquired, lockID, err := pgxadvisorylock.AcquireLockStr(conn, "person:100")
	assert.True(acquired)
	assert.NoError(err)

	released, err := pgxadvisorylock.ReleaseLock(conn, lockID)
	assert.True(released)
	assert.NoError(err)
}
