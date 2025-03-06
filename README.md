# sqlrows-go

`sqlrows-go` is a lightweight utility library that provides an interface,
`RowSet`, to wrap Go’s `*sql.Rows`. This allows you to mock SQL query
results easily for testing purposes.

## Usage

1. **In Production Code**: After executing a query and receiving a 
   `*sql.Rows`, wrap it with `NewRowSet()` to convert it into a `RowSet`.
   Use the `RowSet` interface just like you would `*sql.Rows`.

2. **In Test Code**: Use `NewMockRowSet()` to create a mock query
   result. Pass this mock `RowSet` to your production code during unit
   testing.

This library does not provide a full SQL driver mock like some other
solutions. Instead, it requires your production code to be structured
to associate each query with its corresponding result explicitly.
It’s best suited for testing functions that perform a single SQL query
or wrap the `Query()` calls with a homebrew method.

## Example

Below is an example of how to use `sqlrows-go` with a production
Snowflake query, wrapping `*sql.Rows` into a `RowSet`, and returning
it for use elsewhere.

### Production Code
```go
package myapp

import (
    "database/sql"
    "github.com/jimsnab/sqlrows"
)

// FetchUserBalance retrieves a user's balance from Snowflake.
func FetchUserBalance(db *sql.DB, userID string) (sqlrows.RowSet, error) {
    // Example Snowflake query
    query := `
        SELECT user_id, balance
        FROM accounts
        WHERE user_id = ?
        LIMIT 1
    `
    rows, err := db.Query(query, userID)
    if err != nil {
        return nil, err
    }
    // Wrap *sql.Rows with RowSet
    return sqlrows.NewRowSet(rows), nil
}

// ProcessBalance uses the RowSet to extract data.
func ProcessBalance(rs sqlrows.RowSet) (string, float64, error) {
    defer rs.Close()
    if !rs.Next() {
        return "", 0, sql.ErrNoRows
    }
    var userID string
    var balance float64
    err := rs.Scan(&userID, &balance)
    if err != nil {
        return "", 0, err
    }
    return userID, balance, nil
}
```

### Test Code
```go
package myapp_test

import (
    "database/sql"
    "testing"
    "github.com/jimsnab/sqlrows"
    "github.com/stretchr/testify/assert"
)

func TestProcessBalance(t *testing.T) {
    // Create a mock RowSet for testing
    mockRows := sqlrows.NewMockRowSet([]string{
        "name=user_id;type=string",
        "name=balance;type=float64",
    }, sqlrows.DbTypeSnowflake)
    mockRows.Add(map[string]any{
        "user_id": "12345",
        "balance": 100.50,
    })

    // Call the production code with the mock
    userID, balance, err := ProcessBalance(mockRows)
    assert.NoError(t, err)
    assert.Equal(t, "12345", userID)
    assert.Equal(t, 100.50, balance)
}
```