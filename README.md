pgadvisorylock
===

> NOTE: This is pretty much a work-in-progress, not production ready etc..

Go library for acquiring and releasing [PostgreSQL's Advisory Locks](https://www.postgresql.org/docs/13/explicit-locking.html#ADVISORY-LOCKS) with added support for [pgx](https://github.com/jackc/pgx).

Example Usage

```go
package main

import (
    "github.com/zikani03/pgadvisorylock"
)

func main() {
    // conn is *sql.DB wherever you get your flavour from
    ok, id, err := pgadvisorylock.AcquireLock(conn, "person:1")
    if !ok {
        panic("Failed to acquire lock")
    }

    ok, err := pgadvisorylock.ReleaseLock(conn, id)
    if !ok {
        panic("Failed to release lock")
    }

    ok, id, err := pgadvisorylock.AcquireSharedLock(conn, "person:1")
    if !ok {
        panic("Failed to acquire lock")
    }

    ok, err := pgadvisorylock.ReleaseSharedLock(conn, id)
    if !ok {
        panic("Failed to release lock")
    }
}
```

---

Copyright (c) Zikani