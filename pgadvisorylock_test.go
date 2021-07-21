package pgadvisorylock_test

import (
	"context"
	"testing"

	"github.com/rubenv/pgtest"
	"github.com/stretchr/testify/assert"
	pgadvisorylock "github.com/zikani03/pgadvisorylock"
)

func TestAdvisoryLock(t *testing.T) {
	assert := assert.New(t)

	conn, err := pgtest.Start()
	defer conn.Stop()
	assert.NoError(err)

	ctx := context.Background()

	acquired, lockID, err := pgadvisorylock.AcquireLock(conn.DB, ctx, "person:100")
	assert.True(acquired)
	assert.NoError(err)

	released, err := pgadvisorylock.ReleaseLock(conn.DB, ctx, lockID)
	assert.True(released)
	assert.NoError(err)
}

func TestFetchAdvisoryLocksWithStringLocks(t *testing.T) {
	assert := assert.New(t)
	pg, err := pgtest.Start()
	defer pg.Stop()
	assert.NoError(err)

	ctx := context.Background()

	acquired, lock1, err := pgadvisorylock.AcquireLock(pg.DB, ctx, "person:v1:1000")
	assert.NoError(err)
	assert.True(acquired)

	advisoryLocks, err := pgadvisorylock.FetchAdvisoryLocks(pg.DB, ctx)
	assert.NoError(err)

	assert.Len(advisoryLocks, 1)
	assert.NotEqual(lock1, advisoryLocks[0].ObjectID)

	pgadvisorylock.ReleaseLock(pg.DB, ctx, lock1)

	advisoryLocks2, err := pgadvisorylock.FetchAdvisoryLocks(pg.DB, ctx)
	assert.Len(advisoryLocks2, 0)
}

func TestFetchAdvisoryLocks(t *testing.T) {
	assert := assert.New(t)
	pg, err := pgtest.Start()
	defer pg.Stop()
	assert.NoError(err)
	lock1 := int64(5_00)
	lock2 := int64(1_000_000)

	assert.NotEqual(lock1, lock2)

	ctx := context.Background()

	acquired, err := pgadvisorylock.AcquireLockInt64(pg.DB, ctx, lock1)
	assert.NoError(err)
	assert.True(acquired)

	acquired, err = pgadvisorylock.AcquireLockInt64(pg.DB, ctx, lock2)
	assert.NoError(err)
	assert.True(acquired)

	advisoryLocks, err := pgadvisorylock.FetchAdvisoryLocks(pg.DB, ctx)
	assert.NoError(err)

	assert.Len(advisoryLocks, 2)

	for _, l := range advisoryLocks {
		assert.Greater(l.ObjectID, int64(0))
	}

	pgadvisorylock.ReleaseLock(pg.DB, ctx, lock1)
	pgadvisorylock.ReleaseLock(pg.DB, ctx, lock2)

	advisoryLocks2, err := pgadvisorylock.FetchAdvisoryLocks(pg.DB, ctx)
	assert.Len(advisoryLocks2, 0)
}
