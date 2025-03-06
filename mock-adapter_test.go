package sqlrows

import (
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestMockRowSetSnowflakeBasic tests basic column creation for Snowflake
func TestMockRowSetSnowflakeBasic(t *testing.T) {
	it := newTestCommon(t).
		HasMockRowSet([]string{"name=ID;type=int64", "name=NAME;type=string"}, DbTypeSnowflake)

	it.VerifiesColumns([]string{"ID", "NAME"}).
		VerifiesColumnTypes([]testColumnType{
			{"ID", reflect.TypeOf(int64(0)), "BIGINT", false, 0, 0, 0},
			{"NAME", reflect.TypeOf(""), "VARCHAR", false, 16777216, 0, 0},
		})
}

// TestMockRowSetPostgresPointer tests a pointer type for PostgreSQL
func TestMockRowSetPostgresPointer(t *testing.T) {
	it := newTestCommon(t).
		HasMockRowSet([]string{"name=TS;type=*time.Time"}, DbTypePostgresSQL)

	it.VerifiesColumns([]string{"TS"}).
		VerifiesColumnTypes([]testColumnType{
			{"TS", reflect.PointerTo(reflect.TypeOf(time.Time{})), "TIMESTAMP WITH TIME ZONE", true, 0, 0, 0},
		})
}

// TestMockRowSetMSSQLWithLength tests a string with length for MS SQL
func TestMockRowSetMSSQLWithLength(t *testing.T) {
	it := newTestCommon(t).
		HasMockRowSet([]string{"name=Desc;type=string;length=255"}, DbTypeMsSQL)

	it.VerifiesColumns([]string{"Desc"}).
		VerifiesColumnTypes([]testColumnType{
			{"Desc", reflect.TypeOf(""), "NVARCHAR(MAX)", false, 255, 0, 0},
		})
}

// TestMockRowSetMissingName tests error handling for missing name
func TestMockRowSetMissingName(t *testing.T) {
	it := newTestCommon(t).
		HooksPanic().
		HasMockRowSet([]string{"type=int"}, DbTypeSnowflake)

	it.ExpectedPanic("column spec missing required 'name'")
}

// TestMockRowSetMissingType tests error handling for missing type
func TestMockRowSetMissingType(t *testing.T) {
	it := newTestCommon(t).
		HooksPanic().
		HasMockRowSet([]string{"name=ID"}, DbTypePostgresSQL)

	it.ExpectedPanic("column spec missing required 'type'")
}

// TestMockRowSetInvalidLength tests error handling for invalid length
func TestMockRowSetInvalidLength(t *testing.T) {
	it := newTestCommon(t).
		HooksPanic().
		HasMockRowSet([]string{"name=ID;type=int;length=abc"}, DbTypeMsSQL)

	it.ExpectedPanic("invalid length in column spec")
}

// TestMockRowSetDuplicateColumn tests error handling for duplicate column
func TestMockRowSetDuplicateColumn(t *testing.T) {
	it := newTestCommon(t).
		HooksPanic().
		HasMockRowSet([]string{"name=ID;type=int", "name=ID;type=string"}, DbTypeSnowflake)

	it.ExpectedPanic("duplicate column name in mock row set")
}

// TestMockRowSetAddAndScan tests adding rows and scanning them
func TestMockRowSetAddAndScan(t *testing.T) {
	it := newTestCommon(t)

	it.HasMockRowSet([]string{
		"name=ID;type=int64",
		"name=NAME;type=string",
		"name=TS;type=*time.Time",
	}, DbTypeSnowflake).
		AddsRows(
			map[string]any{"ID": int64(1), "NAME": "Test1", "TS": &it.now},
			map[string]any{"ID": int64(2), "NAME": "Test2", "TS": nil},
		)

	it.VerifiesScan(
		[]any{int64(1), "Test1", &it.now},
		[]any{int64(2), "Test2", nil},
	)
}

// TestMockRowSetAddInvalidColumn tests adding a row with an invalid column
func TestMockRowSetAddInvalidColumn(t *testing.T) {
	it := newTestCommon(t).
		HasMockRowSet([]string{"name=ID;type=int"}, DbTypePostgresSQL).
		HooksPanic().
		AddsRows(map[string]any{"XYZ": 123})

	it.ExpectedPanic("column XYZ does not exist")
}

// TestMockRowSetScanErrors tests scanning behavior and exhaustion
func TestMockRowSetScanErrors(t *testing.T) {
	it := newTestCommon(t).
		HasMockRowSet([]string{"name=ID;type=int;dbType=INTEGER"}, DbTypeMsSQL).
		AddsRows(map[string]any{"ID": 42})

	it.VerifiesScan([]any{42}).
		VerifiesScanExhausted()
}

func TestMockRowSetNextResultSet(t *testing.T) {
	it := newTestCommon(t).
		HasMockRowSet([]string{"name=ID;type=int"}, DbTypeSnowflake).
		AddsRows(
			map[string]any{"ID": 1},
			map[string]any{"ID": 2},
		)

	// Verify initial state: no next result set by default
	mrs := it.rs.(*mockRowSet)
	assert.False(it.t, it.rs.NextResultSet(), "Expected no next result set initially")

	// Set hasNextSet to true to simulate multiple result sets
	mrs.hasNextSet = true
	assert.True(it.t, it.rs.NextResultSet(), "Expected next result set after setting hasNextSet")
	assert.False(it.t, it.rs.NextResultSet(), "Expected no next result set after first call")

	// Verify rows are still accessible after NextResultSet calls
	it.VerifiesScan(
		[]any{1},
		[]any{2},
	)
}

func TestMockColumnTypeDatabaseTypeName(t *testing.T) {
	it := newTestCommon(t).
		HasMockRowSet([]string{
			"name=ID;type=int64",                // Default database type from dbTypesSnowflake
			"name=NAME;type=string",             // Default database type
			"name=TS;type=*time.Time;dbType=TIMESTAMP_NTZ", // Explicit dbType override
		}, DbTypeSnowflake)

	// Retrieve column types
	colTypes, err := it.rs.ColumnTypes()
	require.NoError(it.t, err)
	assert.Equal(it.t, 3, len(colTypes), "Expected 3 column types")

	// Verify DatabaseTypeName for each column
	assert.Equal(it.t, "BIGINT", colTypes[0].DatabaseTypeName(), "ID should have BIGINT database type")
	assert.Equal(it.t, "VARCHAR", colTypes[1].DatabaseTypeName(), "NAME should have VARCHAR database type")
	assert.Equal(it.t, "TIMESTAMP_NTZ", colTypes[2].DatabaseTypeName(), "TS should have TIMESTAMP_NTZ database type")
}

func TestMockColumnTypeDecimalSize(t *testing.T) {
	it := newTestCommon(t).
		HasMockRowSet([]string{
			"name=Price;type=float64;precision=10;scale=2", // Explicit precision and scale
			"name=Amount;type=float64",                     // No precision/scale specified, defaults to 0
			"name=ID;type=int64",                           // Non-decimal type, defaults to 0
		}, DbTypeSnowflake)

	// Retrieve column types
	colTypes, err := it.rs.ColumnTypes()
	require.NoError(it.t, err)
	assert.Equal(it.t, 3, len(colTypes), "Expected 3 column types")

	// Verify DecimalSize for each column
	precision, scale, ok := colTypes[0].DecimalSize() // Price
	assert.Equal(it.t, int64(10), precision, "Price precision should be 10")
	assert.Equal(it.t, int64(2), scale, "Price scale should be 2")
	assert.True(it.t, ok, "Price should have valid decimal size")

	precision, scale, ok = colTypes[1].DecimalSize() // Amount
	assert.Equal(it.t, int64(0), precision, "Amount precision should be 0")
	assert.Equal(it.t, int64(0), scale, "Amount scale should be 0")
	assert.False(it.t, ok, "Amount should not have valid decimal size")

	precision, scale, ok = colTypes[2].DecimalSize() // ID
	assert.Equal(it.t, int64(0), precision, "ID precision should be 0")
	assert.Equal(it.t, int64(0), scale, "ID scale should be 0")
	assert.False(it.t, ok, "ID should not have valid decimal size")
}

func TestMockColumnTypeLength(t *testing.T) {
	it := newTestCommon(t).
		HasMockRowSet([]string{
			"name=NAME;type=string;length=64",
			"name=DESCRIPTION;type=string",
			"name=ID;type=int64",
		}, DbTypeSnowflake)

	colTypes, err := it.rs.ColumnTypes()
	require.NoError(it.t, err)
	assert.Equal(it.t, 3, len(colTypes), "Expected 3 column types")

	length, ok := colTypes[0].Length()
	assert.Equal(it.t, int64(64), length, "NAME length should be 64")
	assert.True(it.t, ok, "NAME should have valid length")

	length, ok = colTypes[1].Length()
	assert.Equal(it.t, int64(16777216), length, "DESCRIPTION length should be 16777216 (Snowflake VARCHAR default)")
	assert.True(it.t, ok, "DESCRIPTION should have valid length")

	length, ok = colTypes[2].Length()
	assert.Equal(it.t, int64(0), length, "ID length should be 0")
	assert.False(it.t, ok, "ID should not have valid length")
}

func TestMockColumnTypeName(t *testing.T) {
	it := newTestCommon(t).
		HasMockRowSet([]string{
			"name=UserID;type=int64",
			"name=FullName;type=string",
			"name=last_updated;type=*time.Time",
		}, DbTypePostgresSQL)

	colTypes, err := it.rs.ColumnTypes()
	require.NoError(it.t, err)
	assert.Equal(it.t, 3, len(colTypes), "Expected 3 column types")

	assert.Equal(it.t, "UserID", colTypes[0].Name(), "UserID should match column name")
	assert.Equal(it.t, "FullName", colTypes[1].Name(), "FullName should match column name")
	assert.Equal(it.t, "last_updated", colTypes[2].Name(), "last_updated should match column name")
}

func TestMockRowSetScanWithoutNext(t *testing.T) {
	it := newTestCommon(t).
		HasMockRowSet([]string{"name=ID;type=int"}, DbTypeSnowflake).
		AddsRows(map[string]any{"ID": 42})

	// Attempt Scan without calling Next
	var id int
	err := it.rs.Scan(&id)
	assert.Error(it.t, err, "Expected error when Scan called without Next")
	assert.Equal(it.t, "sql: Scan called without calling Next", err.Error(), "Error message should match sql.Rows")

	// Verify Next and Scan work after
	assert.True(it.t, it.rs.Next(), "Expected row available after initial Next")
	err = it.rs.Scan(&id)
	assert.NoError(it.t, err, "Scan should succeed after Next")
	assert.Equal(it.t, 42, id, "ID should be 42")

	// Verify no more rows
	assert.False(it.t, it.rs.Next(), "Expected no more rows after scanning all")
}