package sqlrows

import (
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

type (
	MockRowSet interface {
		RowSet
		Add(row map[string]any)
		AddRow(values []any)
	}

	DatabaseType int

	mockRowSet struct {
		order       map[string]struct{}
		orderLwr    map[string]int
		columns     []string
		columnTypes []*mockColumnType
		values      [][]any
		pos         int
		err         error
		hasNextSet  bool
	}

	mockColumnType struct {
		colName      string
		colType      reflect.Type
		nullable     bool
		length       int64
		precision    int64
		scale        int64
		databaseType string
	}
)

const (
	DbTypeSnowflake DatabaseType = iota
	DbTypePostgresSQL
	DbTypeMsSQL
)

var onPanic = func(errMsg string) { panic(errMsg) }

// Creates a mock table, where [cols] is semicolon-separated list of <keyword>=<value>,
// where <keyword> choices are:
//
//	name     	The column name (required)
//	type        The Go type for the column (required)
//	length      Field length (optional)
//	precision   Decimal precision (optional)
//	scale       Field scale (optional)
//	dbType      Name of the database type (optional)
//
// Examples:
//
//		"name=UPDATE_TS;type=*time.Time"
//	    "name=KEY;type=uuid.UUID"
//	    "name=NAME;type=string;length=64"
func NewMockRowSet(cols []string, dbType DatabaseType) MockRowSet {
	row := mockRowSet{
		order:    map[string]struct{}{},
		orderLwr: map[string]int{},
	}

	for _, colSpec := range cols {
		parseColumnSpec(colSpec, dbType, &row)
	}

	return &row
}

func (set *mockRowSet) Add(row map[string]any) {
	vals := make([]any, len(set.columns))
	for k, v := range row {
		colIndex, valid := set.orderLwr[strings.ToLower(k)]
		if !valid {
			onPanic(fmt.Sprintf("column %s does not exist", k))
			return
		}

		vals[colIndex] = v
	}
	set.values = append(set.values, vals)
}

func (set *mockRowSet) AddRow(values []any) {
	vals := make([]any, len(set.columns))
	copy(vals, values)
	set.values = append(set.values, vals)
}

func (m *mockColumnType) DatabaseTypeName() string {
	return m.databaseType
}

func (m *mockColumnType) DecimalSize() (precision int64, scale int64, ok bool) {
	return m.precision, m.scale, m.precision != 0 || m.scale != 0
}

func (m *mockColumnType) Length() (length int64, ok bool) {
	return m.length, m.length != 0
}

func (m *mockColumnType) Name() string {
	return m.colName
}

func (m *mockColumnType) Nullable() (nullable bool, ok bool) {
	return m.nullable, true
}

func (m *mockColumnType) ScanType() reflect.Type {
	return m.colType
}

func (m *mockRowSet) Close() error {
	return nil
}

func (m *mockRowSet) ColumnTypes() ([]ColumnType, error) {
	list := make([]ColumnType, 0, len(m.columnTypes))
	for _, ct := range m.columnTypes {
		list = append(list, ct)
	}
	return list, nil
}

func (m *mockRowSet) Columns() ([]string, error) {
	return m.columns, nil
}

func (m *mockRowSet) Err() error {
	return m.err
}

func (m *mockRowSet) Next() bool {
	if m.pos < len(m.values) {
		m.pos++
		return true
	}
	m.pos = len(m.values) + 1
	return false
}

func (m *mockRowSet) NextResultSet() bool {
	// Simulate a single result set by default; return false after first call
	if m.hasNextSet {
		m.hasNextSet = false
		return true
	}
	return false
}

func (m *mockRowSet) Scan(dest ...any) error {
	if m.pos == 0 {
		return errors.New("sql: Scan called without calling Next")
	}
	if m.pos > len(m.values) {
		return fmt.Errorf("no more rows")
	}
	if len(dest) != len(m.values[m.pos-1]) {
		return fmt.Errorf("destination length %d does not match row length %d", len(dest), len(m.values[m.pos-1]))
	}
	for i, val := range m.values[m.pos-1] {
		nullable, _ := m.columnTypes[i].Nullable()
		if nullable {
			dest[i] = val
		} else {
			reflect.ValueOf(dest[i]).Elem().Set(reflect.ValueOf(val))
		}
	}
	return nil
}

func parseColumnSpec(colSpec string, dbType DatabaseType, row *mockRowSet) {
	parts := strings.Split(colSpec, ";")
	if len(parts) == 0 {
		onPanic(fmt.Sprintf("empty column specification: %s", colSpec))
		return
	}

	var colName string
	var typeStr string
	var dbTypeStr string
	var length *int64
	var precision *int64
	var scale *int64

	// Parse each key=value pair
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		kv := strings.SplitN(part, "=", 2)
		if len(kv) != 2 {
			onPanic(fmt.Sprintf("invalid key=value pair in column spec: %s", part))
			return
		}
		key := strings.TrimSpace(kv[0])
		value := strings.TrimSpace(kv[1])

		switch key {
		case "name":
			colName = value
		case "type":
			typeStr = value
		case "length":
			l, err := strconv.ParseInt(value, 10, 64)
			if err != nil {
				onPanic(fmt.Sprintf("invalid length in column spec: %s", value))
				return
			}
			length = &l
		case "precision":
			p, err := strconv.ParseInt(value, 10, 64)
			if err != nil {
				onPanic(fmt.Sprintf("invalid precision in column spec: %s", value))
				return
			}
			precision = &p
		case "scale":
			s, err := strconv.ParseInt(value, 10, 64)
			if err != nil {
				onPanic(fmt.Sprintf("invalid scale in column spec: %s", value))
				return
			}
			scale = &s
		case "dbType":
			dbTypeStr = value
		default:
			onPanic(fmt.Sprintf("unknown keyword in column spec: %s", key))
			return
		}
	}

	// Validate required fields
	if colName == "" {
		onPanic(fmt.Sprintf("column spec missing required 'name': %s", colSpec))
		return
	}
	if typeStr == "" {
		onPanic(fmt.Sprintf("column spec missing required 'type': %s", colSpec))
		return
	}

	// Get Go type and default database type
	goColType, defaultDbType, nullable := getColumnType(typeStr, dbType)

	// Use provided dbType if specified, otherwise use the default
	dbColType := defaultDbType
	if dbTypeStr != "" {
		dbColType = dbTypeStr
	}

	// Load defaults
	defaultTable := dbTypeDefaults[dbType]
	if defaultTable == nil {
		onPanic("datatabase type is not valid")
		return
	}
	defaults := defaultTable[dbColType]

	if length == nil {
		length = &defaults.length
	}
	if precision == nil {
		precision = &defaults.precision
	}
	if scale == nil {
		scale = &defaults.scale
	}

	// Create the column type
	colType := &mockColumnType{
		colName:      colName,
		colType:      goColType,
		nullable:     nullable,
		length:       *length,
		precision:    *precision,
		scale:        *scale,
		databaseType: dbColType,
	}

	// Add to mockRowSet
	colNameLwr := strings.ToLower(colName)
	if _, exists := row.orderLwr[colNameLwr]; exists {
		onPanic(fmt.Sprintf("duplicate column name in mock row set: %s", colName))
		return
	}
	index := len(row.columns)
	row.order[colName] = struct{}{}
	row.orderLwr[colNameLwr] = index
	row.columns = append(row.columns, colName)
	row.columnTypes = append(row.columnTypes, colType)

	// Ensure values slice has enough columns
	for i := range row.values {
		if len(row.values[i]) < index+1 {
			newRow := make([]any, index+1)
			copy(newRow, row.values[i])
			row.values[i] = newRow
		}
	}
}

func getColumnType(typeStr string, dbType DatabaseType) (goColType reflect.Type, dbColType string, nullable bool) {
	typeStr = strings.TrimSpace(typeStr)
	isPointer := strings.HasPrefix(typeStr, "*")
	baseType := strings.TrimPrefix(typeStr, "*")

	base := baseTypes[baseType]
	if base == nil {
		onPanic(fmt.Sprintf("unsupported type: %s", typeStr))
		return
	}

	if isPointer {
		goColType = reflect.PointerTo(base)
		nullable = true
	} else {
		goColType = base
	}

	switch dbType {
	case DbTypeSnowflake:
		dbColType = dbTypesSnowflake[baseType]
	case DbTypePostgresSQL:
		dbColType = dbTypesPostgres[baseType]
	case DbTypeMsSQL:
		dbColType = dbTypesMsSql[baseType]
	default:
		onPanic("invalid database type")
		return
	}

	if dbColType == "" {
		onPanic(fmt.Sprintf("database type table out of sync with base type table for base type %s", baseType))
		return
	}

	return
}
