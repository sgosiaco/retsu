package retsu

import (
	"fmt"
	"io"
	"os"
	"testing"
	"time"

	"github.com/sgosiaco/column"
)

const (
	deepFilename  = "deep_snapshot.snappy"
	basicFilename = "basic_snapshot.snappy"
	rawFilename   = "raw.bin"
)

func BenchmarkSnapshot(b *testing.B) {
	// create new collection
	col := column.NewCollection()
	// add columns
	csvCols := map[string]any{
		"index": 0,
		"str":   "0",
	}
	col.CreateColumnsOf(csvCols)

	max := 1_000_000
	// iterate over data and insert in bulk
	col.Query(func(txn *column.Txn) error {
		// range over data
		for i := 0; i < max; i++ {
			txn.Insert(func(r column.Row) error {
				r.SetInt("index", i)
				r.SetString("str", fmt.Sprint(i))
				return nil
			})
		}
		return nil // Commit
	})

	b.Run("raw", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			if err := col.Snapshot(io.Discard); err != nil {
				b.Errorf("Unexpected error: %s", err)
			}
		}
	})

	b.Run("basic", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			if err := SaveBasicSnapshot(col, io.Discard); err != nil {
				b.Errorf("Unexpected error: %s", err)
			}
		}
	})

	b.Run("deep", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			if err := SaveDeepSnapshot(col, csvCols, io.Discard); err != nil {
				b.Errorf("Unexpected error: %s", err)
			}
		}
	})
}

func TestDeepSnapshot(t *testing.T) {
	// create new collection
	col := column.NewCollection()
	// add columns
	csvCols := map[string]any{
		"index": 0,
		"str":   "0",
	}
	col.CreateColumnsOf(csvCols)

	max := 1_000_000
	half := (max / 2)
	expected := (max / 2) - 1

	// iterate over data and insert in bulk
	col.Query(func(txn *column.Txn) error {
		// range over data
		for i := 0; i < max; i++ {
			txn.Insert(func(r column.Row) error {
				r.SetInt("index", i)
				r.SetString("str", fmt.Sprint(i))
				return nil
			})
		}
		return nil // Commit
	})

	curCount := 0
	col.Query(func(txn *column.Txn) error {
		curCount = txn.WithValue("index", func(v interface{}) bool {
			return v.(int) > half
		}).Count()
		return nil
	})

	if curCount != expected {
		t.Errorf("Expected %d, instead found %d", expected, curCount)
	}

	if err := SaveDeepSnapshotFile(col, csvCols, deepFilename); err != nil {
		t.Errorf("Unexpected error: %s", err)
	}

	newCol, _, err := LoadDeepSnapshotFile(deepFilename)
	if err != nil {
		t.Errorf("Unexpected error: %s", err)
	}

	newCount := 0
	newCol.Query(func(txn *column.Txn) error {
		newCount = txn.WithValue("index", func(v interface{}) bool {
			return v.(int) > half
		}).Count()
		return nil
	})

	if newCount != curCount {
		t.Errorf("Expected %d, instead found %d", curCount, newCount)
	}
}

func TestBasicSnapshot(t *testing.T) {
	// create new collection
	col := column.NewCollection()
	// add columns
	csvCols := map[string]any{
		"index": 0,
		"str":   "0",
	}
	col.CreateColumnsOf(csvCols)

	max := 1_000_000
	half := (max / 2)
	expected := (max / 2) - 1

	// iterate over data and insert in bulk
	col.Query(func(txn *column.Txn) error {
		// range over data
		for i := 0; i < max; i++ {
			txn.Insert(func(r column.Row) error {
				r.SetInt("index", i)
				r.SetString("str", fmt.Sprint(i))
				return nil
			})
		}
		return nil // Commit
	})

	curCount := 0
	col.Query(func(txn *column.Txn) error {
		curCount = txn.WithValue("index", func(v interface{}) bool {
			return v.(int) > half
		}).Count()
		return nil
	})

	if curCount != expected {
		t.Errorf("Expected %d, instead found %d", expected, curCount)
	}

	if err := SaveBasicSnapshotFile(col, basicFilename); err != nil {
		t.Errorf("Unexpected error: %s", err)
	}

	// create new collection
	newCol := column.NewCollection()
	// add columns
	newCol.CreateColumnsOf(csvCols)

	err := LoadBasicSnapshotFile(newCol, basicFilename)
	if err != nil {
		t.Errorf("Unexpected error: %s", err)
	}

	newCount := 0
	newCol.Query(func(txn *column.Txn) error {
		newCount = txn.WithValue("index", func(v interface{}) bool {
			return v.(int) > half
		}).Count()
		return nil
	})

	if newCount != curCount {
		t.Errorf("Expected %d, instead found %d", curCount, newCount)
	}
}

func TestRawSnapshot(t *testing.T) {
	// create new collection
	col := column.NewCollection()
	// add columns
	csvCols := map[string]any{
		"index": 0,
		"str":   "0",
	}
	col.CreateColumnsOf(csvCols)

	max := 1_000_000
	half := (max / 2)
	expected := (max / 2) - 1

	// iterate over data and insert in bulk
	col.Query(func(txn *column.Txn) error {
		// range over data
		for i := 0; i < max; i++ {
			txn.Insert(func(r column.Row) error {
				r.SetInt("index", i)
				r.SetString("str", fmt.Sprint(i))
				return nil
			})
		}
		return nil // Commit
	})

	curCount := 0
	col.Query(func(txn *column.Txn) error {
		curCount = txn.WithValue("index", func(v interface{}) bool {
			return v.(int) > half
		}).Count()
		return nil
	})

	if curCount != expected {
		t.Errorf("Expected %d, instead found %d", expected, curCount)
	}

	file, err := os.Create(rawFilename)
	if err != nil {
		t.Errorf("Unexpected error: %s", err)
	}

	if err := col.Snapshot(file); err != nil {
		t.Errorf("Unexpected error: %s", err)
	}
	file.Close()

	// create new collection
	newCol := column.NewCollection()
	// add columns
	newCol.CreateColumnsOf(csvCols)

	file, err = os.Open(rawFilename)
	if err != nil {
		t.Errorf("Unexpected error: %s", err)
	}
	newCol.Restore(file)
	file.Close()

	newCount := 0
	newCol.Query(func(txn *column.Txn) error {
		newCount = txn.WithValue("index", func(v interface{}) bool {
			return v.(int) > half
		}).Count()
		return nil
	})

	if newCount != curCount {
		t.Errorf("Expected %d, instead found %d", curCount, newCount)
	}
}

func TestStructToCols(t *testing.T) {
	type testObj struct {
		Index int    `json:"index"`
		Str   string `json:"str"`
		T     time.Time
	}

	// create new collection
	col := column.NewCollection()
	// add columns
	csvCols, err := StructToCols[testObj](col, "json")
	if err != nil {
		t.Errorf("Unexpected error: %s", err)
	}

	max := 1_000_000
	half := (max / 2)
	expected := (max / 2) - 1

	// iterate over data and insert in bulk
	col.Query(func(txn *column.Txn) error {
		// range over data
		for i := 0; i < max; i++ {
			t := testObj{
				Index: i,
				Str:   fmt.Sprint(i),
			}

			txn.Insert(InsertStruct(&t, "json"))
		}
		return nil // Commit
	})

	curCount := 0
	col.Query(func(txn *column.Txn) error {
		curCount = txn.WithValue("index", func(v interface{}) bool {
			return v.(int) > half
		}).Count()
		return nil
	})

	if curCount != expected {
		t.Errorf("Expected %d, instead found %d", expected, curCount)
	}

	if err := SaveDeepSnapshotFile(col, csvCols, deepFilename); err != nil {
		t.Errorf("Unexpected error: %s", err)
	}

	newCol, _, err := LoadDeepSnapshotFile(deepFilename)
	if err != nil {
		t.Errorf("Unexpected error: %s", err)
	}

	newCount := 0
	newCol.Query(func(txn *column.Txn) error {
		newCount = txn.WithValue("index", func(v interface{}) bool {
			return v.(int) > half
		}).Count()
		return nil
	})

	if newCount != curCount {
		t.Errorf("Expected %d, instead found %d", curCount, newCount)
	}
}
