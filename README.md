# grizz

A Polars-inspired DataFrame library for Go, built on Apache Arrow.

## Features

- **Polars-like API**: Familiar syntax for Polars/Python users
- **Lazy Evaluation**: Build complex queries efficiently with lazy DataFrames
- **Apache Arrow Backend**: Efficient memory handling and zero-copy operations
- **Rich Expression System**: Filter, transform, aggregate with expressions
- **Multiple Data Types**: Int8-64, UInt8-64, Float64, String, Boolean, Binary with null support
- **I/O Support**: CSV, JSON, Parquet, and Arrow IPC read/write
- **Optimizers**: Projection pushdown and Slice pushdown for query optimization
- **Advanced Operations**: Rolling windows, cumulative operations, window functions

## Installation

```bash
go get github.com/ecelayes/grizz
```

## Quick Start

```go
package main

import (
    "github.com/ecelayes/grizz/dataframe"
    "github.com/ecelayes/grizz/engine"
    "github.com/ecelayes/grizz/expr"
    "github.com/ecelayes/grizz/io/csv"
)

func main() {
    // Read CSV
    df, _ := csv.Read("data.csv")
    defer df.Release()

    // Lazy API with filter and select
    result, _ := df.Lazy().
        Filter(expr.Col("age").Gt(expr.Lit(25))).
        Select(expr.Col("name"), expr.Col("age")).
        Collect(engine.Execute)

    defer result.Release()
    result.Show()
}
```

## API Overview

### Data Types

| Type | Go | Description |
|------|-----|-------------|
| Int64 | `int64` | 64-bit integer |
| UInt64/32/16/8 | `uint64/32/16/8` | Unsigned integers |
| Float64 | `float64` | 64-bit float |
| String | `string` | UTF-8 string |
| Boolean | `bool` | Boolean values |
| Binary | `[]byte` | Binary data |

All types support null values via validity bitmaps.

### Expressions

```go
// Column reference
expr.Col("name")

// Literals
expr.Lit(42)
expr.Lit(3.14)
expr.Lit("hello")
expr.Lit(true)

// Arithmetic
expr.Col("a").Add(expr.Col("b"))
expr.Col("x").Sub(expr.Lit(1))
expr.Col("y").Mul(expr.Lit(2))

// Comparisons
expr.Col("age").Eq(expr.Lit(30))
expr.Col("age").Gt(expr.Lit(18))
expr.Col("name").Ne(expr.Lit("John"))
expr.Col("score").LtEq(expr.Lit(100))

// Logical
expr.Col("a").And(expr.Col("b"))
expr.Col("x").Or(expr.Col("y"))
expr.Not(expr.Col("flag"))

// Null handling
expr.IsNull(expr.Col("name"))
expr.IsNotNull(expr.Col("email"))
expr.FillNull(expr.Col("value"), expr.Lit(0))
expr.FillNullForward(expr.Col("value"))
expr.FillNullBackward(expr.Col("value"))
```

### String Operations

```go
expr.Upper(expr.Col("name"))
expr.Lower(expr.Col("name"))
expr.Trim(expr.Col("text"))
expr.Contains(expr.Col("text"), expr.Lit("pattern"))
expr.Replace(expr.Col("text"), expr.Lit("old"), expr.Lit("new"))
expr.Length(expr.Col("name"))

// Additional string operations
expr.Extract(expr.Col("text"), expr.Lit(`(\d+)`))
expr.Find(expr.Col("text"), expr.Lit("substring"))
```

### Aggregations

```go
expr.Sum("salary")
expr.Count("id")
expr.Min("price")
expr.Max("rating")
expr.Mean("score")
```

### Rolling Windows

```go
// Rolling window operations
expr.RollingSum(expr.Col("value"), 7, 7)
expr.RollingMean(expr.Col("value"), 7, 7)
expr.RollingMin(expr.Col("value"), 7, 7)
expr.RollingMax(expr.Col("value"), 7, 7)
```

### Cumulative Operations

```go
// Cumulative operations
expr.CumSum(expr.Col("value"))
expr.CumProd(expr.Col("value"))
expr.CumMin(expr.Col("value"))
expr.CumMax(expr.Col("value"))
```

### Window Functions

```go
expr.RowNumber()
expr.Rank()
expr.Lag(expr.Col("value"), 1)
expr.Lead(expr.Col("value"), 1)
```

### DataFrame Operations

```go
// Filter
df.Filter(expr.Col("age").Gt(expr.Lit(18)))

// Select columns
df.Select(expr.Col("name"), expr.Col("age"))

// With columns (add/modify)
df.WithColumns(
    expr.Col("age").Add(expr.Lit(1)).Alias("age_next_year"),
)

// Drop nulls
df.DropNulls()

// Distinct
df.Distinct()

// Group by
df.GroupBy("department").Agg(expr.Sum("salary"))

// Additional group by operations
df.GroupBy("department").Head(3)
df.GroupBy("department").Tail(3)
df.GroupBy("department").Groups()

// Join
df1.Join(df2, "id", dataframe.Inner)

// Additional join types
df.SemiJoin(df2, "id")
df.AntiJoin(df2, "id")

// Sort
df.Sort("age", false)  // false = ascending

// Limit
df.Limit(100)

// Additional operations
df.Tail(10)
```

### Lazy API

```go
import "github.com/ecelayes/grizz/engine"

// Build lazy query
lazy := df.Lazy().
    Filter(expr.Col("age").Gt(expr.Lit(25))).
    Select(expr.Col("name"), expr.Col("department")).
    GroupBy("department").Agg(expr.Sum("salary"))

// Collect when ready
result, _ := lazy.Collect(engine.Execute)
```

### I/O

```go
// Read
df, _ := csv.Read("file.csv")
df, _ := json.Read("file.json")
df, _ := parquet.Read("file.parquet")
df, _ := arrow.Read("file.arrow")

// Write
csv.Write(df, "output.csv")
json.Write(df, "output.json")
parquet.Write(df, "output.parquet")
arrow.Write(df, "output.arrow")
```

## Architecture

```
grizz/
├── dataframe/    # DataFrame and LazyFrame
├── engine/      # Query execution engine
├── expr/        # Expression system
├── series/      # Column data types
└── io/          # CSV, JSON, Parquet I/O
```

## Contributing

Contributions welcome! Please ensure tests pass before submitting PRs.

```bash
go test ./...
```

## License

MIT
