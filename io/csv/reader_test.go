package csv

import (
	"os"
	"testing"
)

func TestRead(t *testing.T) {
	content := `name,age,salary,active
Alice,30,50000.5,true
Bob,25,45000.0,false
Charlie,35,55000.75,true`

	tmpFile, err := os.CreateTemp("", "test*.csv")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	if _, err := tmpFile.WriteString(content); err != nil {
		t.Fatalf("Failed to write to temp file: %v", err)
	}
	tmpFile.Close()

	df, err := Read(tmpFile.Name())
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	if df.NumRows() != 3 {
		t.Errorf("Expected 3 rows, got %d", df.NumRows())
	}

	if df.NumCols() != 4 {
		t.Errorf("Expected 4 columns, got %d", df.NumCols())
	}
}

func TestReadEmptyFile(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "empty*.csv")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()
	tmpFile.Close()

	_, err = Read(tmpFile.Name())
	if err == nil {
		t.Error("Expected error for empty file")
	}
}

func TestReadNoDataRows(t *testing.T) {
	content := `name,age`

	tmpFile, err := os.CreateTemp("", "header*.csv")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	if _, err := tmpFile.WriteString(content); err != nil {
		t.Fatalf("Failed to write to temp file: %v", err)
	}
	tmpFile.Close()

	_, err = Read(tmpFile.Name())
	if err == nil {
		t.Error("Expected error for file with only header")
	}
}

func TestReadInferTypes(t *testing.T) {
	content := `bool_col,int_col,float_col,string_col
true,42,3.14,hello
false,100,2.71,world`

	tmpFile, err := os.CreateTemp("", "types*.csv")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	if _, err := tmpFile.WriteString(content); err != nil {
		t.Fatalf("Failed to write to temp file: %v", err)
	}
	tmpFile.Close()

	df, err := Read(tmpFile.Name())
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	if df.NumRows() != 2 {
		t.Errorf("Expected 2 rows, got %d", df.NumRows())
	}
}

func TestReadWithNulls(t *testing.T) {
	content := `name,age,salary
Alice,30,50000
Bob,,45000
,25,`

	tmpFile, err := os.CreateTemp("", "nulls*.csv")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	if _, err := tmpFile.WriteString(content); err != nil {
		t.Fatalf("Failed to write to temp file: %v", err)
	}
	tmpFile.Close()

	df, err := Read(tmpFile.Name())
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	if df.NumRows() != 3 {
		t.Errorf("Expected 3 rows, got %d", df.NumRows())
	}
}

func TestReadInferFromAllRows(t *testing.T) {
	content := `value
1
2
three`

	tmpFile, err := os.CreateTemp("", "infer*.csv")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	if _, err := tmpFile.WriteString(content); err != nil {
		t.Fatalf("Failed to write to temp file: %v", err)
	}
	tmpFile.Close()

	df, err := Read(tmpFile.Name())
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	if df.NumRows() != 3 {
		t.Errorf("Expected 3 rows, got %d", df.NumRows())
	}
}

func TestReadFileNotFound(t *testing.T) {
	_, err := Read("/nonexistent/path/to/file.csv")
	if err == nil {
		t.Error("Expected error for non-existent file")
	}
}

func TestReadWithOnlyHeader(t *testing.T) {
	content := `name,age,salary`

	tmpFile, err := os.CreateTemp("", "header*.csv")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	if _, err := tmpFile.WriteString(content); err != nil {
		t.Fatalf("Failed to write to temp file: %v", err)
	}
	tmpFile.Close()

	_, err = Read(tmpFile.Name())
	if err == nil {
		t.Error("Expected error for file with only header")
	}
}

func TestReadWithWhitespace(t *testing.T) {
	content := `name, age , salary
Alice, 30 , 50000
Bob, 25 , 45000`

	tmpFile, err := os.CreateTemp("", "whitespace*.csv")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	if _, err := tmpFile.WriteString(content); err != nil {
		t.Fatalf("Failed to write to temp file: %v", err)
	}
	tmpFile.Close()

	df, err := Read(tmpFile.Name())
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	if df.NumRows() != 2 {
		t.Errorf("Expected 2 rows, got %d", df.NumRows())
	}
}

func TestReadWithSpecialChars(t *testing.T) {
	content := `name,desc
Alice,"hello, world"
Bob,"test
newline"`

	tmpFile, err := os.CreateTemp("", "special*.csv")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	if _, err := tmpFile.WriteString(content); err != nil {
		t.Fatalf("Failed to write to temp file: %v", err)
	}
	tmpFile.Close()

	df, err := Read(tmpFile.Name())
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	if df.NumRows() != 2 {
		t.Errorf("Expected 2 rows, got %d", df.NumRows())
	}
}

func TestReadWithBooleanValues(t *testing.T) {
	content := `flag
true
false
True
False
TRUE
FALSE`

	tmpFile, err := os.CreateTemp("", "bool*.csv")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	if _, err := tmpFile.WriteString(content); err != nil {
		t.Fatalf("Failed to write to temp file: %v", err)
	}
	tmpFile.Close()

	df, err := Read(tmpFile.Name())
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	if df.NumRows() != 6 {
		t.Errorf("Expected 6 rows, got %d", df.NumRows())
	}
}

func TestReadWithFloatValues(t *testing.T) {
	content := `value
1.5
2.7
3.0
.5
0.`

	tmpFile, err := os.CreateTemp("", "float*.csv")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	if _, err := tmpFile.WriteString(content); err != nil {
		t.Fatalf("Failed to write to temp file: %v", err)
	}
	tmpFile.Close()

	df, err := Read(tmpFile.Name())
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	if df.NumRows() != 5 {
		t.Errorf("Expected 5 rows, got %d", df.NumRows())
	}
}

func TestReadWithNegativeNumbers(t *testing.T) {
	content := `value
-1
-100
-3.14
0`

	tmpFile, err := os.CreateTemp("", "negative*.csv")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	if _, err := tmpFile.WriteString(content); err != nil {
		t.Fatalf("Failed to write to temp file: %v", err)
	}
	tmpFile.Close()

	df, err := Read(tmpFile.Name())
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	if df.NumRows() != 4 {
		t.Errorf("Expected 4 rows, got %d", df.NumRows())
	}
}

func TestReadWithBoolOnlyColumn(t *testing.T) {
	content := `flag
true
false
true
false`

	tmpFile, err := os.CreateTemp("", "boolonly*.csv")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	if _, err := tmpFile.WriteString(content); err != nil {
		t.Fatalf("Failed to write to temp file: %v", err)
	}
	tmpFile.Close()

	df, err := Read(tmpFile.Name())
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	if df.NumRows() != 4 {
		t.Errorf("Expected 4 rows, got %d", df.NumRows())
	}
}

func TestReadWithMixedBoolStrings(t *testing.T) {
	content := `value
true
maybe
false
unknown`

	tmpFile, err := os.CreateTemp("", "mixed*.csv")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	if _, err := tmpFile.WriteString(content); err != nil {
		t.Fatalf("Failed to write to temp file: %v", err)
	}
	tmpFile.Close()

	df, err := Read(tmpFile.Name())
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	if df.NumRows() != 4 {
		t.Errorf("Expected 4 rows, got %d", df.NumRows())
	}
}

func TestReadBoolOnlyColumn(t *testing.T) {
	content := `flag
true
false
true`

	tmpFile, err := os.CreateTemp("", "boolonly*.csv")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	if _, err := tmpFile.WriteString(content); err != nil {
		t.Fatalf("Failed to write: %v", err)
	}
	tmpFile.Close()

	df, err := Read(tmpFile.Name())
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	if df.NumRows() != 3 {
		t.Errorf("Expected 3 rows, got %d", df.NumRows())
	}
}

func TestReadWithNumericAndFloatMixed(t *testing.T) {
	content := `value
1
2.5
3
4.5`

	tmpFile, err := os.CreateTemp("", "test*.csv")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	if _, err := tmpFile.WriteString(content); err != nil {
		t.Fatalf("Failed to write: %v", err)
	}
	tmpFile.Close()

	df, err := Read(tmpFile.Name())
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	if df.NumRows() != 4 {
		t.Errorf("Expected 4 rows, got %d", df.NumRows())
	}
}

func TestReadWithNumericStrings(t *testing.T) {
	content := `value
123
456
789
abc`

	tmpFile, err := os.CreateTemp("", "numeric*.csv")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	if _, err := tmpFile.WriteString(content); err != nil {
		t.Fatalf("Failed to write to temp file: %v", err)
	}
	tmpFile.Close()

	df, err := Read(tmpFile.Name())
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	if df.NumRows() != 4 {
		t.Errorf("Expected 4 rows, got %d", df.NumRows())
	}
}

func TestReadWithMultipleColumnsAllTypes(t *testing.T) {
	content := `bool_col,int_col,float_col,str_col,bool2
true,42,3.14,hello,true
false,100,2.71,world,false
yes,0,0.0,test,true`

	tmpFile, err := os.CreateTemp("", "multitype*.csv")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	if _, err := tmpFile.WriteString(content); err != nil {
		t.Fatalf("Failed to write to temp file: %v", err)
	}
	tmpFile.Close()

	df, err := Read(tmpFile.Name())
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	if df.NumRows() != 3 {
		t.Errorf("Expected 3 rows, got %d", df.NumRows())
	}
	if df.NumCols() != 5 {
		t.Errorf("Expected 5 columns, got %d", df.NumCols())
	}
}

func TestReadSingleRow(t *testing.T) {
	content := `name,age
Alice,30`

	tmpFile, err := os.CreateTemp("", "singlerow*.csv")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	if _, err := tmpFile.WriteString(content); err != nil {
		t.Fatalf("Failed to write to temp file: %v", err)
	}
	tmpFile.Close()

	df, err := Read(tmpFile.Name())
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	if df.NumRows() != 1 {
		t.Errorf("Expected 1 row, got %d", df.NumRows())
	}
}

func TestReadWithFloatsNotInt(t *testing.T) {
	content := `value
1.5
2.5
3.5`

	tmpFile, err := os.CreateTemp("", "floatonly*.csv")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	if _, err := tmpFile.WriteString(content); err != nil {
		t.Fatalf("Failed to write to temp file: %v", err)
	}
	tmpFile.Close()

	df, err := Read(tmpFile.Name())
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	if df.NumRows() != 3 {
		t.Errorf("Expected 3 rows, got %d", df.NumRows())
	}
}

func TestReadWithEmptyColumn(t *testing.T) {
	content := `name,value
Alice,
Bob,10`

	tmpFile, err := os.CreateTemp("", "test*.csv")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	if _, err := tmpFile.WriteString(content); err != nil {
		t.Fatalf("Failed to write: %v", err)
	}
	tmpFile.Close()

	df, err := Read(tmpFile.Name())
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	if df.NumRows() != 2 {
		t.Errorf("Expected 2 rows, got %d", df.NumRows())
	}
}

func TestReadAllTypesMixed(t *testing.T) {
	content := `intCol,floatCol,boolCol,strCol
1,1.5,true,hello
2,2.5,false,world
3,3.5,true,test`

	tmpFile, err := os.CreateTemp("", "test*.csv")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	if _, err := tmpFile.WriteString(content); err != nil {
		t.Fatalf("Failed to write: %v", err)
	}
	tmpFile.Close()

	df, err := Read(tmpFile.Name())
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	if df.NumRows() != 3 {
		t.Errorf("Expected 3 rows, got %d", df.NumRows())
	}
	if df.NumCols() != 4 {
		t.Errorf("Expected 4 columns, got %d", df.NumCols())
	}
}

func TestReadWithInvalidNumbers(t *testing.T) {
	content := `name,value
Alice,abc
Bob,def`

	tmpFile, err := os.CreateTemp("", "test*.csv")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	if _, err := tmpFile.WriteString(content); err != nil {
		t.Fatalf("Failed to write: %v", err)
	}
	tmpFile.Close()

	df, err := Read(tmpFile.Name())
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	if df.NumRows() != 2 {
		t.Errorf("Expected 2 rows, got %d", df.NumRows())
	}
}

func TestReadOnlyHeader(t *testing.T) {
	content := `name,age`

	tmpFile, err := os.CreateTemp("", "test*.csv")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	if _, err := tmpFile.WriteString(content); err != nil {
		t.Fatalf("Failed to write: %v", err)
	}
	tmpFile.Close()

	_, err = Read(tmpFile.Name())
	if err == nil {
		t.Errorf("Expected error for CSV with only header")
	}
}

func TestReadAllNullColumn(t *testing.T) {
	content := `name,value
Alice,
Bob,
Charlie,`

	tmpFile, err := os.CreateTemp("", "test*.csv")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	if _, err := tmpFile.WriteString(content); err != nil {
		t.Fatalf("Failed to write: %v", err)
	}
	tmpFile.Close()

	df, err := Read(tmpFile.Name())
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	if df.NumRows() != 3 {
		t.Errorf("Expected 3 rows, got %d", df.NumRows())
	}
}

func TestReadBoolColumnWithInvalidValues(t *testing.T) {
	content := `flag
true
invalid
false`

	tmpFile, err := os.CreateTemp("", "test*.csv")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	if _, err := tmpFile.WriteString(content); err != nil {
		t.Fatalf("Failed to write: %v", err)
	}
	tmpFile.Close()

	df, err := Read(tmpFile.Name())
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	if df.NumRows() != 3 {
		t.Errorf("Expected 3 rows, got %d", df.NumRows())
	}
}

func TestReadIntColumnWithInvalidValues(t *testing.T) {
	content := `value
1
abc
3`

	tmpFile, err := os.CreateTemp("", "test*.csv")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	if _, err := tmpFile.WriteString(content); err != nil {
		t.Fatalf("Failed to write: %v", err)
	}
	tmpFile.Close()

	df, err := Read(tmpFile.Name())
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	if df.NumRows() != 3 {
		t.Errorf("Expected 3 rows, got %d", df.NumRows())
	}
}

func TestReadFloatColumnWithInvalidValues(t *testing.T) {
	content := `value
1.5
xyz
3.5`

	tmpFile, err := os.CreateTemp("", "test*.csv")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	if _, err := tmpFile.WriteString(content); err != nil {
		t.Fatalf("Failed to write: %v", err)
	}
	tmpFile.Close()

	df, err := Read(tmpFile.Name())
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	if df.NumRows() != 3 {
		t.Errorf("Expected 3 rows, got %d", df.NumRows())
	}
}

func TestReadBoolAndIntMixed(t *testing.T) {
	content := `flag
true
1
false
0`

	tmpFile, err := os.CreateTemp("", "test*.csv")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	if _, err := tmpFile.WriteString(content); err != nil {
		t.Fatalf("Failed to write: %v", err)
	}
	tmpFile.Close()

	df, err := Read(tmpFile.Name())
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	if df.NumRows() != 4 {
		t.Errorf("Expected 4 rows, got %d", df.NumRows())
	}
}

func TestReadColumnAllEmptyBecomesString(t *testing.T) {
	content := `name,value
Alice,1
Bob,
Charlie,`

	tmpFile, err := os.CreateTemp("", "test*.csv")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	if _, err := tmpFile.WriteString(content); err != nil {
		t.Fatalf("Failed to write: %v", err)
	}
	tmpFile.Close()

	df, err := Read(tmpFile.Name())
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	if df.NumRows() != 3 {
		t.Errorf("Expected 3 rows, got %d", df.NumRows())
	}
}

func TestReadBoolOnlyColumnType(t *testing.T) {
	content := `flag
true
false
true`

	tmpFile, err := os.CreateTemp("", "test*.csv")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	if _, err := tmpFile.WriteString(content); err != nil {
		t.Fatalf("Failed to write: %v", err)
	}
	tmpFile.Close()

	df, err := Read(tmpFile.Name())
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	if df.NumRows() != 3 {
		t.Errorf("Expected 3 rows, got %d", df.NumRows())
	}
}

func TestReadBoolNotIntNotFloat(t *testing.T) {
	content := `value
true
false
true`

	tmpFile, err := os.CreateTemp("", "test*.csv")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	if _, err := tmpFile.WriteString(content); err != nil {
		t.Fatalf("Failed to write: %v", err)
	}
	tmpFile.Close()

	df, err := Read(tmpFile.Name())
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	if df.NumRows() != 3 {
		t.Errorf("Expected 3 rows, got %d", df.NumRows())
	}
}

func TestReadWithLeadingTrailingWhitespace(t *testing.T) {
	content := `  name  , age , score 
  Alice , 30 , 100.5 
  Bob , 25 , 95.0`

	tmpFile, err := os.CreateTemp("", "test*.csv")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	if _, err := tmpFile.WriteString(content); err != nil {
		t.Fatalf("Failed to write: %v", err)
	}
	tmpFile.Close()

	df, err := Read(tmpFile.Name())
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	if df.NumRows() != 2 {
		t.Errorf("Expected 2 rows, got %d", df.NumRows())
	}
}

func TestReadEmptyValuesInFirstRows(t *testing.T) {
	content := `name,value
,1
,2
Alice,3`

	tmpFile, err := os.CreateTemp("", "test*.csv")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	if _, err := tmpFile.WriteString(content); err != nil {
		t.Fatalf("Failed to write: %v", err)
	}
	tmpFile.Close()

	df, err := Read(tmpFile.Name())
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	if df.NumRows() != 3 {
		t.Errorf("Expected 3 rows, got %d", df.NumRows())
	}
}

func TestReadIntColumnInferred(t *testing.T) {
	content := `value
1
2
3`

	tmpFile, err := os.CreateTemp("", "test*.csv")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	if _, err := tmpFile.WriteString(content); err != nil {
		t.Fatalf("Failed to write: %v", err)
	}
	tmpFile.Close()

	df, err := Read(tmpFile.Name())
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	if df.NumRows() != 3 {
		t.Errorf("Expected 3 rows, got %d", df.NumRows())
	}
}

func TestReadFloatColumnInferred(t *testing.T) {
	content := `value
1.5
2.5
3.5`

	tmpFile, err := os.CreateTemp("", "test*.csv")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	if _, err := tmpFile.WriteString(content); err != nil {
		t.Fatalf("Failed to write: %v", err)
	}
	tmpFile.Close()

	df, err := Read(tmpFile.Name())
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	if df.NumRows() != 3 {
		t.Errorf("Expected 3 rows, got %d", df.NumRows())
	}
}

func TestReadStringFromBoolNotIntNotFloat(t *testing.T) {
	content := `value
yes
no
maybe`

	tmpFile, err := os.CreateTemp("", "test*.csv")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	if _, err := tmpFile.WriteString(content); err != nil {
		t.Fatalf("Failed to write: %v", err)
	}
	tmpFile.Close()

	df, err := Read(tmpFile.Name())
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	if df.NumRows() != 3 {
		t.Errorf("Expected 3 rows, got %d", df.NumRows())
	}
}

func TestReadColumnTypeStringFromMixedBoolAndEmpty(t *testing.T) {
	content := `value
true
false
`

	tmpFile, err := os.CreateTemp("", "test*.csv")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	if _, err := tmpFile.WriteString(content); err != nil {
		t.Fatalf("Failed to write: %v", err)
	}
	tmpFile.Close()

	df, err := Read(tmpFile.Name())
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	if df.NumRows() != 2 {
		t.Errorf("Expected 2 rows, got %d", df.NumRows())
	}
}

func TestReadWithInvalidCSVContent(t *testing.T) {
	content := `name,age
"unclosed quote`

	tmpFile, err := os.CreateTemp("", "invalid*.csv")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	if _, err := tmpFile.WriteString(content); err != nil {
		t.Fatalf("Failed to write: %v", err)
	}
	tmpFile.Close()

	_, err = Read(tmpFile.Name())
	if err == nil {
		t.Error("Expected error for invalid CSV content")
	}
}

func TestReadBoolInferredColumn(t *testing.T) {
	content := `flag
true
false
true`

	tmpFile, err := os.CreateTemp("", "test*.csv")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	if _, err := tmpFile.WriteString(content); err != nil {
		t.Fatalf("Failed to write: %v", err)
	}
	tmpFile.Close()

	df, err := Read(tmpFile.Name())
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	if df.NumRows() != 3 {
		t.Errorf("Expected 3 rows, got %d", df.NumRows())
	}
}

func TestReadWithBoolParseError(t *testing.T) {
	content := `flag
invalid_value
true`

	tmpFile, err := os.CreateTemp("", "test*.csv")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	if _, err := tmpFile.WriteString(content); err != nil {
		t.Fatalf("Failed to write: %v", err)
	}
	tmpFile.Close()

	df, err := Read(tmpFile.Name())
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	if df.NumRows() != 2 {
		t.Errorf("Expected 2 rows, got %d", df.NumRows())
	}
}

func TestReadWithIntParseError(t *testing.T) {
	content := `value
1
abc
3`

	tmpFile, err := os.CreateTemp("", "test*.csv")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	if _, err := tmpFile.WriteString(content); err != nil {
		t.Fatalf("Failed to write: %v", err)
	}
	tmpFile.Close()

	df, err := Read(tmpFile.Name())
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	if df.NumRows() != 3 {
		t.Errorf("Expected 3 rows, got %d", df.NumRows())
	}
}

func TestReadWithFloatParseError(t *testing.T) {
	content := `value
1.5
abc
3.5`

	tmpFile, err := os.CreateTemp("", "test*.csv")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	if _, err := tmpFile.WriteString(content); err != nil {
		t.Fatalf("Failed to write: %v", err)
	}
	tmpFile.Close()

	df, err := Read(tmpFile.Name())
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	if df.NumRows() != 3 {
		t.Errorf("Expected 3 rows, got %d", df.NumRows())
	}
}

func TestReadBoolWithEmpty(t *testing.T) {
	content := `flag
true
false
`

	tmpFile, err := os.CreateTemp("", "test*.csv")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	if _, err := tmpFile.WriteString(content); err != nil {
		t.Fatalf("Failed to write: %v", err)
	}
	tmpFile.Close()

	df, err := Read(tmpFile.Name())
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	if df.NumRows() != 2 {
		t.Errorf("Expected 2 rows, got %d", df.NumRows())
	}
}

func TestReadBoolWithInvalid(t *testing.T) {
	content := `flag
true
invalid
false`

	tmpFile, err := os.CreateTemp("", "test*.csv")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	if _, err := tmpFile.WriteString(content); err != nil {
		t.Fatalf("Failed to write: %v", err)
	}
	tmpFile.Close()

	df, err := Read(tmpFile.Name())
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	if df.NumRows() != 3 {
		t.Errorf("Expected 3 rows, got %d", df.NumRows())
	}
}

func TestReadBoolWithEmptyInMiddle(t *testing.T) {
	content := `flag
true
false`

	tmpFile, err := os.CreateTemp("", "test*.csv")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	if _, err := tmpFile.WriteString(content); err != nil {
		t.Fatalf("Failed to write: %v", err)
	}
	tmpFile.Close()

	df, err := Read(tmpFile.Name())
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	if df.NumRows() != 2 {
		t.Errorf("Expected 2 rows, got %d", df.NumRows())
	}
}

func TestReadIntWithEmptyInMiddle(t *testing.T) {
	content := `value
1
2`

	tmpFile, err := os.CreateTemp("", "test*.csv")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	if _, err := tmpFile.WriteString(content); err != nil {
		t.Fatalf("Failed to write: %v", err)
	}
	tmpFile.Close()

	df, err := Read(tmpFile.Name())
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	if df.NumRows() != 2 {
		t.Errorf("Expected 2 rows, got %d", df.NumRows())
	}
}

func TestReadFloatWithEmptyInMiddle(t *testing.T) {
	content := `value
1.5
2.5`

	tmpFile, err := os.CreateTemp("", "test*.csv")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	if _, err := tmpFile.WriteString(content); err != nil {
		t.Fatalf("Failed to write: %v", err)
	}
	tmpFile.Close()

	df, err := Read(tmpFile.Name())
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	if df.NumRows() != 2 {
		t.Errorf("Expected 2 rows, got %d", df.NumRows())
	}
}
