package json

import (
	"os"
	"testing"
)

func TestRead(t *testing.T) {
	content := `[
		{"name": "Alice", "age": 30, "active": true},
		{"name": "Bob", "age": 25, "active": false}
	]`

	tmpFile, err := os.CreateTemp("", "test*.json")
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

	if df.NumCols() != 3 {
		t.Errorf("Expected 3 columns, got %d", df.NumCols())
	}
}

func TestReadEmptyFile(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "empty*.json")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()
	tmpFile.WriteString("[]")
	tmpFile.Close()

	_, err = Read(tmpFile.Name())
	if err == nil {
		t.Error("Expected error for empty array")
	}
}

func TestReadNestedJSON(t *testing.T) {
	content := `[
		{"name": "Alice", "profile": {"city": "NYC", "country": "USA"}},
		{"name": "Bob", "profile": {"city": "LA", "country": "USA"}}
	]`

	tmpFile, err := os.CreateTemp("", "nested*.json")
	if err != nil {
		t.Fatalf("Failed to create temp: %v", err)
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

	if df.NumCols() != 2 {
		t.Errorf("Expected 2 columns, got %d", df.NumCols())
	}
}

func TestReadArrayOfObjects(t *testing.T) {
	content := `[
		{"name": "Alice", "tags": ["developer", "go"]},
		{"name": "Bob", "tags": ["designer", "ui"]}
	]`

	tmpFile, err := os.CreateTemp("", "array*.json")
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

	if df.NumCols() != 2 {
		t.Errorf("Expected 2 columns, got %d", df.NumCols())
	}
}

func TestReadWithNullValues(t *testing.T) {
	content := `[
		{"name": "Alice", "age": 30},
		{"name": null, "age": 25},
		{"name": "Charlie", "age": null}
	]`

	tmpFile, err := os.CreateTemp("", "test*.json")
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

	nameCol, _ := df.ColByName("name")
	if !nameCol.IsNull(1) {
		t.Errorf("Expected null at row 1 for name column")
	}

	ageCol, _ := df.ColByName("age")
	if !ageCol.IsNull(2) {
		t.Errorf("Expected null at row 2 for age column")
	}
}

func TestReadFileNotFound(t *testing.T) {
	_, err := Read("/nonexistent/path/to/file.json")
	if err == nil {
		t.Error("Expected error for non-existent file")
	}
}

func TestReadInvalidJSON(t *testing.T) {
	content := `{invalid json`

	tmpFile, err := os.CreateTemp("", "invalid*.json")
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
		t.Error("Expected error for invalid JSON")
	}
}

func TestReadWithAllNullColumn(t *testing.T) {
	content := `[
		{"name": "Alice"},
		{"name": null},
		{"name": null}
	]`

	tmpFile, err := os.CreateTemp("", "test*.json")
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

	nameCol, _ := df.ColByName("name")
	if !nameCol.IsNull(1) || !nameCol.IsNull(2) {
		t.Errorf("Expected nulls at rows 1 and 2")
	}
}

func TestReadWithMixedNumbers(t *testing.T) {
	content := `[
		{"value": 1},
		{"value": 2.5},
		{"value": 3}
	]`

	tmpFile, err := os.CreateTemp("", "test*.json")
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

func TestReadWithBooleanInferred(t *testing.T) {
	content := `[
		{"flag": true},
		{"flag": false},
		{"flag": true}
	]`

	tmpFile, err := os.CreateTemp("", "test*.json")
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

func TestReadWithMissingKey(t *testing.T) {
	content := `[
		{"name": "Alice", "age": 30},
		{"name": "Bob"},
		{"name": "Charlie", "age": 25}
	]`

	tmpFile, err := os.CreateTemp("", "test*.json")
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

func TestReadWithNumericZero(t *testing.T) {
	content := `[
		{"value": 0},
		{"value": 1},
		{"value": 0}
	]`

	tmpFile, err := os.CreateTemp("", "test*.json")
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

func TestReadWithBoolZero(t *testing.T) {
	content := `[
		{"flag": false},
		{"flag": true},
		{"flag": false}
	]`

	tmpFile, err := os.CreateTemp("", "test*.json")
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

func TestReadWithNumericAndBoolMixed(t *testing.T) {
	content := `[
		{"val": 1},
		{"val": true},
		{"val": 0}
	]`

	tmpFile, err := os.CreateTemp("", "test*.json")
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

func TestReadWithSingleRow(t *testing.T) {
	content := `[{"name": "Alice", "age": 30}]`

	tmpFile, err := os.CreateTemp("", "test*.json")
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

	if df.NumRows() != 1 {
		t.Errorf("Expected 1 row, got %d", df.NumRows())
	}
}

func TestReadWithExplicitNull(t *testing.T) {
	content := `[
		{"name": "Alice"},
		{"name": null},
		{"name": "Bob"}
	]`

	tmpFile, err := os.CreateTemp("", "test*.json")
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

	nameCol, _ := df.ColByName("name")
	if !nameCol.IsNull(1) {
		t.Errorf("Expected null at row 1")
	}
}

func TestReadWithFloatInferred(t *testing.T) {
	content := `[
		{"value": 1},
		{"value": 2.5},
		{"value": 3}
	]`

	tmpFile, err := os.CreateTemp("", "test*.json")
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
