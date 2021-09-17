pgadvisorylock
===

Go library for acquiring and releasing [PostgreSQL's Advisory Locks](https://www.postgresql.org/docs/13/explicit-locking.html#ADVISORY-LOCKS) with added support for [pgx](https://github.com/jackc/pgx).


## Use in your project

```sh
$ go get github.com/zikani03/pgadvisorylock
```

## Example Usage

```go
package main

import (
    "context"
    "github.com/zikani03/pgadvisorylock"
)

func main() {
    // conn is *sql.DB wherever you get your flavour from
    ctx := context.Context
    ok, id, err := pgadvisorylock.AcquireLock(conn, ctx, "person:1")
    if !ok {
        panic("Failed to acquire lock")
    }

    ok, err = pgadvisorylock.ReleaseLock(conn, ctx, id)
    if !ok {
        panic("Failed to release lock")
    }

    ok, id, err = pgadvisorylock.AcquireSharedLock(conn, ctx, "person:1")
    if !ok {
        panic("Failed to acquire lock")
    }


    advisoryLocks, err = pgadvisorylock.FetchAdvisoryLocks(conn, ctx)
    if err != nil {
        panic("Failed to fetch locks")
    }

    for _, l := range advisoryLocks {
        fmt.Printf("LockID:%s, ClassID:%s, PID:%s\n", string(l.ObjectID), string(l.ClassID), string(l.PID))
    }


    ok, err = pgadvisorylock.ReleaseSharedLock(conn, ctx, id)
    if !ok {
        panic("Failed to release lock")
    }
}
```

---

Copyright (c) Zikani Nyirenda Mwase