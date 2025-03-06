package sqlrows

import (
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type (
	testCommon struct {
		t         *testing.T
		rs        MockRowSet
		now       time.Time
		uuid      uuid.UUID
		rowsAdded int
		panicMsg  string
	}

	testColumnType struct {
		colName   string
		goType    reflect.Type
		dbType    string
		nullable  bool
		length    int64
		precision int64
		scale     int64
	}
)

func newTestCommon(t *testing.T) *testCommon {
	return &testCommon{
		t:    t,
		now:  time.Now(),
		uuid: uuid.New(),
	}
}

func (it *testCommon) HasMockRowSet(cols []string, dbType DatabaseType) *testCommon {
	it.rs = NewMockRowSet(cols, dbType)
	return it
}

func (it *testCommon) AddsRows(rows ...map[string]any) *testCommon {
	for _, row := range rows {
		it.rs.Add(row)
		it.rowsAdded++
	}
	return it
}

func (it *testCommon) VerifiesColumns(expectedCols []string) *testCommon {
	cols, err := it.rs.Columns()
	require.NoError(it.t, err)
	assert.Equal(it.t, expectedCols, cols, "Columns do not match expected")
	return it
}

func (it *testCommon) VerifiesColumnTypes(expectedTypes []testColumnType) *testCommon {
	colTypes, err := it.rs.ColumnTypes()
	require.NoError(it.t, err)
	assert.Equal(it.t, len(expectedTypes), len(colTypes), "Number of column types does not match")

	for i, exp := range expectedTypes {
		ct := colTypes[i].(*mockColumnType)
		assert.Equal(it.t, exp.colName, ct.colName)
		assert.Equal(it.t, exp.goType, ct.colType)
		assert.Equal(it.t, exp.dbType, ct.databaseType)
		assert.Equal(it.t, exp.nullable, ct.nullable)
		assert.Equal(it.t, exp.length, ct.length)
		assert.Equal(it.t, exp.precision, ct.precision)
		assert.Equal(it.t, exp.scale, ct.scale)
	}
	return it
}

func (it *testCommon) VerifiesScan(expectedRows ...[]any) *testCommon {
	mrs := it.rs.(*mockRowSet)
	assert.Equal(it.t, it.rowsAdded, len(mrs.values), "Number of rows added does not match")

	mrs.pos = 0
	for i := range it.rowsAdded {
		assert.True(it.t, it.rs.Next(), "Expected more rows to scan")
		var dest = make([]any, len(mrs.columns))
		for j := range dest {
			switch mrs.columnTypes[j].ScanType().Kind() {
			case reflect.Int:
				var v int
				dest[j] = &v
			case reflect.Int64:
				var v int64
				dest[j] = &v
			case reflect.String:
				var v string
				dest[j] = &v
			case reflect.Ptr:
				dest[j] = new(time.Time)
			default:
				it.t.Fatalf("unsupported type for scan: %v", mrs.columnTypes[j].ScanType())
			}
		}
		err := it.rs.Scan(dest...)
		require.NoError(it.t, err)
		for j, val := range dest {
			expected := expectedRows[i][j]
			nullable, _ := mrs.columnTypes[j].Nullable()
			if nullable {
				assert.Equal(it.t, expected, val, "Row %d, column %d does not match", i, j)
			} else {
				actual := reflect.ValueOf(val).Elem().Interface()
				assert.Equal(it.t, expected, actual, "Row %d, column %d does not match", i, j)
			}
		}
	}
	assert.False(it.t, it.rs.Next(), "Expected no more rows")
	return it
}

func (it *testCommon) HooksPanic() *testCommon {
	org := onPanic
	onPanic = func(errMsg string) {
		if it.panicMsg != "" {
			panic("multiple panics - must ensure nothing else runs after calling onPanic")
		}
		if errMsg == "" {
			panic("a panic message is required")
		}
		it.panicMsg = errMsg
	}
	it.t.Cleanup(func() {
		onPanic = org
	})
	return it
}

func (it *testCommon) ExpectedPanic(expectedErr string) *testCommon {
	assert.Contains(it.t, it.panicMsg, expectedErr)
	return it
}

func (it *testCommon) VerifiesScanExhausted() *testCommon {
	assert.False(it.t, it.rs.Next(), "Expected no more rows after scanning")
	var dummy int
	err := it.rs.Scan(&dummy)
	assert.Error(it.t, err, "Expected error when scanning past end")
	if err != nil {
		assert.Contains(it.t, err.Error(), "no more rows")
	}
	return it
}
