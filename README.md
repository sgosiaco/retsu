# retsu 列

## A library to provide convenience functions for [column](https://github.com/kelindar/column)

## Sample Usage

```go
type testObj struct {
	Index int    `json:"index"`
	Str   string `json:"str"`
}

// create new collection
col := column.NewCollection()

// add columns from struct
csvCols, err := StructToCols[testObj](col, "json")
if err != nil {
	return err
}

// add data
col.Query(func(txn *column.Txn) error {
  t := testObj{
    Index: 1,
    Str: "1",
  }
  // insert row based on struct
  txn.Insert(InsertStruct(&t, "json"))
  return nil
})

// save deep snapshot
if err := SaveDeepSnapshotFile(col, csvCols, "deep_snapshot.snappy"); err != nil {
	return err
}

---

// load deep snapshot
savedCol, savedCSVCols, err := LoadDeepSnapshotFile("deep_snapshot.snappy")
if err != nil {
	return err
}
```

## Benchmarks
```
cpu: AMD Ryzen 9 5900X 12-Core Processor            
BenchmarkSnapshot/raw-24         	      57	  19821002 ns/op	12077931 B/op	     457 allocs/op
BenchmarkSnapshot/basic-24       	      61	  19677610 ns/op	13691278 B/op	     524 allocs/op
BenchmarkSnapshot/deep-24        	      55	  21546802 ns/op	23765586 B/op	     555 allocs/op
```