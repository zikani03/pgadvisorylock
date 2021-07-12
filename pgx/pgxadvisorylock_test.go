package pgx_test

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v4"
	"github.com/stretchr/testify/assert"
	pgxadvisorylock "github.com/zikani03/pgadvisorylock/pgx"
)

func TestPgxAdvisoryLock(t *testing.T) {
	assert := assert.New(t)

	conn, err := pgx.Connect(context.Background(), "dbname=postgres")
	defer conn.Close(context.Background())

	assert.NoError(err)
	//var lockID int64
	acquired, lockID, err := pgxadvisorylock.AcquireLock(conn, "person:100")
	assert.True(acquired)
	assert.NoError(err)

	released, err := pgxadvisorylock.ReleaseLock(conn, lockID)
	assert.True(released)
	assert.NoError(err)
}
