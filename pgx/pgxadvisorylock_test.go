package pgx_test

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	pgxadvisorylock "github.com/zikani03/pgadvisorylock/pgx"
)

func TestPgxAdvisoryLock(t *testing.T) {
	assert := assert.New(t)

	ctx := context.Background()
	conn, err := pgx.Connect(ctx, "dbname=postgres")
	defer conn.Close(ctx)

	assert.NoError(err)
	//var lockID int64
	acquired, lockID, err := pgxadvisorylock.AcquireLock(conn, ctx, "person:100")
	assert.True(acquired)
	assert.NoError(err)

	released, err := pgxadvisorylock.ReleaseLock(conn, ctx, lockID)
	assert.True(released)
	assert.NoError(err)
}
