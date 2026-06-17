module github.com/vkost/go-ds-sql

go 1.24.0

require (
	github.com/ipfs/go-datastore v0.9.1
	github.com/lib/pq v1.3.0
	github.com/mattn/go-sqlite3 v1.14.9
	github.com/pkg/errors v0.9.1
	github.com/textileio/go-datastore-extensions v1.1.0
)

require (
	github.com/google/uuid v1.6.0 // indirect
	github.com/ipfs/go-detect-race v0.0.1 // indirect
)

replace github.com/textileio/go-datastore-extensions v1.1.0 => github.com/vkost/go-datastore-extensions v1.1.1
