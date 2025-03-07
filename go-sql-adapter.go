package sqlrows

import (
	"database/sql"
	"reflect"
)

type (
	RowSet interface {
		Close() error
		ColumnTypes() ([]ColumnType, error)
		Columns() ([]string, error)
		Err() error
		Next() bool
		NextResultSet() bool
		Scan(dest ...any) error
	}

	ColumnType interface {
		DatabaseTypeName() string
		DecimalSize() (precision int64, scale int64, ok bool)
		Length() (length int64, ok bool)
		Name() string
		Nullable() (nullable bool, ok bool)
		ScanType() reflect.Type
	}

	sqlRowsWrapper struct {
		inner *sql.Rows
	}

	sqlColTypeWrapper struct {
		inner *sql.ColumnType
	}
)

func NewRowSet(rows *sql.Rows) RowSet {
	return &sqlRowsWrapper{inner: rows}
}

func (rows *sqlRowsWrapper) Close() error {
	return rows.inner.Close()
}

func (rows *sqlRowsWrapper) ColumnTypes() ([]ColumnType, error) {
	colTypes, err := rows.inner.ColumnTypes()
	if err != nil {
		return nil, err
	}

	wrapped := make([]ColumnType, 0, len(colTypes))
	for _, colType := range colTypes {
		wrapped = append(wrapped, &sqlColTypeWrapper{inner: colType})
	}

	return wrapped, nil
}

func (rows *sqlRowsWrapper) Columns() ([]string, error) {
	return rows.inner.Columns()
}

func (rows *sqlRowsWrapper) Err() error {
	return rows.inner.Err()
}

func (rows *sqlRowsWrapper) Next() bool {
	return rows.inner.Next()
}

func (rows *sqlRowsWrapper) NextResultSet() bool {
	return rows.inner.NextResultSet()
}

func (rows *sqlRowsWrapper) Scan(dest ...any) error {
	return rows.inner.Scan(dest...)
}

func (colType *sqlColTypeWrapper) DatabaseTypeName() string {
	return colType.inner.DatabaseTypeName()
}

func (colType *sqlColTypeWrapper) DecimalSize() (precision int64, scale int64, ok bool) {
	return colType.inner.DecimalSize()
}

func (colType *sqlColTypeWrapper) Length() (length int64, ok bool) {
	return colType.inner.Length()
}

func (colType *sqlColTypeWrapper) Name() string {
	return colType.inner.Name()
}

func (colType *sqlColTypeWrapper) Nullable() (nullable bool, ok bool) {
	return colType.inner.Nullable()
}

func (colType *sqlColTypeWrapper) ScanType() reflect.Type {
	return colType.inner.ScanType()
}
