# SQL Datastore

An implementation of [the datastore interface](https://github.com/ipfs/go-datastore)
that can be backed by any sql database.

## Usage

```go
import (
	"database/sql"
	"github.com/ipfs/go-ds-sql"
)

mydb, _ := sql.Open("yourdb", "yourdbparameters")

ds := sqlds.NewDatastore(mydb)
```

## License
MIT
