package pgadvisorylock_test

import (
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
	//var lockID int64
	acquired, lockID, err := pgadvisorylock.AcquireLock(conn.DB, "person:100")
	assert.True(acquired)
	assert.NoError(err)

	released, err := pgadvisorylock.ReleaseLock(conn.DB, lockID)
	assert.True(released)
	assert.NoError(err)
}
