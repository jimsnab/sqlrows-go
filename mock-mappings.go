package sqlrows

import (
	"reflect"
	"time"

	"github.com/google/uuid"
)

// databaseDefaults defines default length, precision, and scale for SQL data types across databases
type databaseDefaults struct {
	length    int64 // Default length for string types (e.g., VARCHAR); 0 if not applicable
	precision int64 // Default precision for numeric types; 0 if not applicable
	scale     int64 // Default scale for numeric types; 0 if not applicable
}

// dbTypeDefaults maps database type and SQL type to their default values
var dbTypeDefaults = map[DatabaseType]map[string]databaseDefaults{
	DbTypeSnowflake: {
		"VARCHAR":   {length: 16777216, precision: 0, scale: 0}, // 16 MB max
		"NUMBER":    {length: 0, precision: 38, scale: 0},       // Alias for DECIMAL
		"DECIMAL":   {length: 0, precision: 38, scale: 0},
		"INTEGER":   {length: 0, precision: 0, scale: 0}, // Fixed size, no defaults needed
		"BIGINT":    {length: 0, precision: 0, scale: 0},
		"FLOAT":     {length: 0, precision: 0, scale: 0}, // IEEE 754, no precision/scale
		"DOUBLE":    {length: 0, precision: 0, scale: 0},
		"BOOLEAN":   {length: 0, precision: 0, scale: 0},
		"TIMESTAMP": {length: 0, precision: 0, scale: 0},
	},
	DbTypePostgresSQL: {
		"TEXT":                     {length: 1073741824, precision: 0, scale: 0}, // 1 GB max (TEXT has no length limit by default)
		"VARCHAR":                  {length: 1073741824, precision: 0, scale: 0}, // Same as TEXT if unspecified
		"NUMERIC":                  {length: 0, precision: 0, scale: 0},          // Unlimited unless specified
		"DECIMAL":                  {length: 0, precision: 0, scale: 0},          // Alias for NUMERIC
		"INTEGER":                  {length: 0, precision: 0, scale: 0},
		"BIGINT":                   {length: 0, precision: 0, scale: 0},
		"SMALLINT":                 {length: 0, precision: 0, scale: 0},
		"REAL":                     {length: 0, precision: 6, scale: 0},  // 6 decimal digits
		"DOUBLE precision":         {length: 0, precision: 15, scale: 0}, // 15 decimal digits
		"BOOLEAN":                  {length: 0, precision: 0, scale: 0},
		"TIMESTAMP WITH TIME ZONE": {length: 0, precision: 0, scale: 0},
		"UUID":                     {length: 0, precision: 0, scale: 0},
	},
	DbTypeMsSQL: {
		"NVARCHAR(MAX)":    {length: 2147483647, precision: 0, scale: 0}, // 2^31-1 characters
		"VARCHAR(MAX)":     {length: 2147483647, precision: 0, scale: 0}, // 2^31-1 bytes
		"DECIMAL":          {length: 0, precision: 18, scale: 0},
		"NUMERIC":          {length: 0, precision: 18, scale: 0}, // Alias for DECIMAL
		"INT":              {length: 0, precision: 0, scale: 0},
		"BIGINT":           {length: 0, precision: 0, scale: 0},
		"SMALLINT":         {length: 0, precision: 0, scale: 0},
		"TINYINT":          {length: 0, precision: 0, scale: 0},
		"REAL":             {length: 0, precision: 7, scale: 0},  // 7 decimal digits
		"FLOAT":            {length: 0, precision: 15, scale: 0}, // 15 decimal digits
		"BIT":              {length: 0, precision: 0, scale: 0},
		"DATETIME2":        {length: 0, precision: 0, scale: 0},
		"UNIQUEIDENTIFIER": {length: 0, precision: 0, scale: 0},
	},
}

// Map of base type names to their reflect.Type
var baseTypes = map[string]reflect.Type{
	"bool":       reflect.TypeOf(false),
	"int":        reflect.TypeOf(0),
	"int8":       reflect.TypeOf(int8(0)),
	"int16":      reflect.TypeOf(int16(0)),
	"int32":      reflect.TypeOf(int32(0)),
	"int64":      reflect.TypeOf(int64(0)),
	"uint":       reflect.TypeOf(uint(0)),
	"uint8":      reflect.TypeOf(uint8(0)),
	"uint16":     reflect.TypeOf(uint16(0)),
	"uint32":     reflect.TypeOf(uint32(0)),
	"uint64":     reflect.TypeOf(uint64(0)),
	"float32":    reflect.TypeOf(float32(0)),
	"float64":    reflect.TypeOf(float64(0)),
	"complex64":  reflect.TypeOf(complex64(0)),
	"complex128": reflect.TypeOf(complex128(0)),
	"string":     reflect.TypeOf(""),
	"byte":       reflect.TypeOf(byte(0)),
	"rune":       reflect.TypeOf(rune(0)),
	"uintptr":    reflect.TypeOf(uintptr(0)),
	"time.Time":  reflect.TypeOf(time.Time{}),
	"uuid.UUID":  reflect.TypeOf(uuid.UUID{}),
}

var dbTypesSnowflake = map[string]string{
	"bool":       "BOOLEAN",
	"int":        "INTEGER",
	"int8":       "INTEGER", // Snowflake doesn’t have TINYINT, smallest is INTEGER
	"int16":      "INTEGER", // No SMALLINT, use INTEGER
	"int32":      "INTEGER",
	"int64":      "BIGINT",
	"uint":       "INTEGER", // No unsigned support; use INTEGER or BIGINT based on range
	"uint8":      "INTEGER", // No unsigned; INTEGER can handle 0-255
	"uint16":     "INTEGER", // No unsigned; INTEGER can handle 0-65535
	"uint32":     "BIGINT",  // No unsigned; INTEGER max is 2^31-1, use BIGINT for full range
	"uint64":     "BIGINT",  // No unsigned; BIGINT max is 2^63-1, sufficient for most uint64
	"float32":    "FLOAT",
	"float64":    "DOUBLE",  // Snowflake uses DOUBLE for 64-bit floats
	"complex64":  "VARCHAR", // No complex type; store as string
	"complex128": "VARCHAR", // No complex type; store as string
	"string":     "VARCHAR",
	"byte":       "INTEGER", // No direct BYTE; INTEGER for 0-255 range
	"rune":       "INTEGER", // Rune is int32, maps to INTEGER
	"uintptr":    "BIGINT",  // Pointer size varies, BIGINT is safe
	"time.Time":  "TIMESTAMP",
	"uuid.UUID":  "VARCHAR", // Snowflake doesn’t have UUID type; use VARCHAR (36 chars typical)
}

var dbTypesPostgres = map[string]string{
	"bool":       "BOOLEAN",
	"int":        "INTEGER",
	"int8":       "SMALLINT",
	"int16":      "SMALLINT",
	"int32":      "INTEGER",
	"int64":      "BIGINT",
	"uint":       "INTEGER",
	"uint8":      "SMALLINT",
	"uint16":     "INTEGER",
	"uint32":     "BIGINT",
	"uint64":     "NUMERIC(20)",
	"float32":    "REAL",
	"float64":    "DOUBLE PRECISION",
	"complex64":  "TEXT",
	"complex128": "TEXT",
	"string":     "TEXT",
	"byte":       "SMALLINT",
	"rune":       "INTEGER",
	"uintptr":    "BIGINT",
	"time.Time":  "TIMESTAMP WITH TIME ZONE",
	"uuid.UUID":  "UUID",
}

var dbTypesMsSql = map[string]string{
	"bool":       "BIT",
	"int":        "INT",
	"int8":       "TINYINT",
	"int16":      "SMALLINT",
	"int32":      "INT",
	"int64":      "BIGINT",
	"uint":       "INT",         // No unsigned; INT covers 0 to 2^31-1
	"uint8":      "TINYINT",     // 0 to 255 fits perfectly
	"uint16":     "INT",         // SMALLINT only goes to 32767, use INT
	"uint32":     "BIGINT",      // INT max is 2^31-1, use BIGINT
	"uint64":     "DECIMAL(20)", // BIGINT max is 2^63-1, use DECIMAL for full range
	"float32":    "REAL",
	"float64":    "FLOAT",
	"complex64":  "NVARCHAR(MAX)", // No complex type; store as string
	"complex128": "NVARCHAR(MAX)", // No complex type; store as string
	"string":     "NVARCHAR(MAX)",
	"byte":       "TINYINT",
	"rune":       "INT", // Rune is int32, maps to INT
	"uintptr":    "BIGINT",
	"time.Time":  "DATETIME2",
	"uuid.UUID":  "UNIQUEIDENTIFIER",
}
