package retsu

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"os"
	"reflect"
	"strings"

	"github.com/kelindar/column"
	"github.com/klauspost/compress/s2"
	"github.com/sgosiaco/retsu/internal/pool"
)

// pools
var (
	_bufPool = pool.New(func() *bytes.Buffer {
		return new(bytes.Buffer)
	})

	_s2WriterPool = pool.New(func() *s2.Writer {
		return s2.NewWriter(nil)
	})

	_s2ReaderPool = pool.New(func() *s2.Reader {
		return s2.NewReader(nil)
	})
)

var (
	errContinue = errors.New("continue")
)

// SaveBasicSnapshotFile takes a collection and creates a basic snapshot (only data, no columns)
// Saves it to filepath using s2 compression
func SaveBasicSnapshotFile(col *column.Collection, filepath string) error {
	file, err := os.Create(filepath)
	if err != nil {
		return err
	}
	defer file.Close()

	return SaveBasicSnapshot(col, file)
}

// SaveBasicSnapshot takes a collection and creates a basic snapshot (only data, no columns)
// Writes to given writer using s2 compression
func SaveBasicSnapshot(col *column.Collection, w io.Writer) error {
	// get s2 writer from pool
	s2enc := _s2WriterPool.Get()
	// reset after retrieving from pool
	s2enc.Reset(w)
	// put back into pool once done using
	defer _s2WriterPool.Put(s2enc)

	// write snapshot to s2
	if err := col.Snapshot(s2enc); err != nil {
		return err
	}

	// close s2 to write to writer
	return s2enc.Close()
}

// LoadBasicSnapshotFile takes a collection and loads a basic snapshot (only data, no columns)
// Loads it from the given filepath using s2 decompression
func LoadBasicSnapshotFile(col *column.Collection, filepath string) error {
	file, err := os.Open(filepath)
	if err != nil {
		return err
	}
	defer file.Close()

	return LoadBasicSnapshot(col, file)
}

// LoadBasicSnapshot takes a collection and loads a basic snapshot (only data, no columns)
// Reads from given reader using s2 decompression
func LoadBasicSnapshot(col *column.Collection, r io.Reader) error {
	// get s2 reader from pool
	s2dec := _s2ReaderPool.Get()
	// reset after retrieving from pool
	s2dec.Reset(r)
	// put back into pool once done using
	defer _s2ReaderPool.Put(s2dec)

	// restore from s2
	return col.Restore(s2dec)
}

// DeepSnapshot struct to hold csv column data and collection data
type DeepSnapshot struct {
	CSVCols map[string]any
	Data    []byte
}

// ToCollection converts the deep snapshot back into a collection + columns
func (d DeepSnapshot) ToCollection() (*column.Collection, map[string]any, error) {
	// create new collection
	col := column.NewCollection()
	// add columns
	if err := col.CreateColumnsOf(d.CSVCols); err != nil {
		return nil, nil, err
	}
	// restore from data
	if err := col.Restore(bytes.NewReader(d.Data)); err != nil {
		return nil, d.CSVCols, err
	}

	return col, d.CSVCols, nil
}

// SaveDeepSnapshotFile takes a collection and creates a deep snapshot (data + columns)
// Saves it to filepath using gob encoding + s2 compression
func SaveDeepSnapshotFile(col *column.Collection, csvCols map[string]any, filepath string) error {
	file, err := os.Create(filepath)
	if err != nil {
		return err
	}
	defer file.Close()

	return SaveDeepSnapshot(col, csvCols, file)
}

// SaveDeepSnapshot takes a collection and creates a deep snapshot (data + columns)
// Writes to given writer using gob encoding + s2 compression
func SaveDeepSnapshot(col *column.Collection, csvCols map[string]any, w io.Writer) error {
	// get bytes buffer from pool
	buf := _bufPool.Get()
	// reset after retrieving from pool
	buf.Reset()
	// put back into pool once done using
	defer _bufPool.Put(buf)

	// write snapshot to temp buffer
	if err := col.Snapshot(buf); err != nil {
		return err
	}

	// create deep snapshot
	ds := DeepSnapshot{
		CSVCols: csvCols,
		Data:    buf.Bytes(),
	}

	// get s2 writer from pool
	s2enc := _s2WriterPool.Get()
	// reset after retrieving from pool
	s2enc.Reset(w)
	// put back into pool once done using
	defer _s2WriterPool.Put(s2enc)

	// gob encode deep snapshot into s2
	enc := gob.NewEncoder(s2enc)
	enc.Encode(ds)

	// close s2 to write to writer
	return s2enc.Close()
}

// LoadDeepSnapshotFile takes a collection and loads a deep snapshot (data + columns)
// Loads it from the given filepath using gob decoding + s2 decompression
func LoadDeepSnapshotFile(filepath string) (*column.Collection, map[string]any, error) {
	file, err := os.Open(filepath)
	if err != nil {
		return nil, nil, err
	}

	return LoadDeepSnapshot(file)
}

// LoadDeepSnapshot takes a collection and loads a deep snapshot (data + columns)
// Reads from given reader using gob decoding + s2 decompression
func LoadDeepSnapshot(r io.Reader) (*column.Collection, map[string]any, error) {
	// get s2 reader from pool
	s2dec := _s2ReaderPool.Get()
	// reset after retrieving from pool
	s2dec.Reset(r)
	// put back into pool once done using
	defer _s2ReaderPool.Put(s2dec)

	// gob decode deep snapshot from s2
	dec := gob.NewDecoder(s2dec)
	var ds DeepSnapshot
	if err := dec.Decode(&ds); err != nil {
		return nil, nil, err
	}

	// convert deep snapshot back into collection
	return ds.ToCollection()
}

// StructToColsMap converts a given struct to map of column name to value
// Fields must be tagged with given structTag
func StructToColsMap[T any](structTag string) (map[string]any, error) {
	n := new(T)
	cols := make(map[string]any)

	err := ExecWhenSettable(n, structTag, func(tag string, v reflect.Value) error {
		val, err := GetValue(v)
		if err != nil {
			fmt.Printf("Field [%s] has unsupported column type: %s\n", tag, v.Kind())
			return errContinue
		}

		if _, exists := cols[tag]; exists {
			return fmt.Errorf("duplicate tag: %s", tag)
		}

		cols[tag] = val
		return nil
	})

	return cols, err
}

// StructToCols converts a given struct to columns
// Fields must be tagged with given structTag
func StructToCols[T any](col *column.Collection, structTag string) (map[string]any, error) {
	cols, err := StructToColsMap[T](structTag)
	if err != nil {
		return nil, err
	}
	if err := col.CreateColumnsOf(cols); err != nil {
		return nil, err
	}

	return cols, nil
}

// StructToColsDirect converts a given struct to columns
// Fields must be tagged with given structTag
func StructToColsDirect[T any](col *column.Collection, structTag string) error {
	return ExecWhenSettable(new(T), structTag, func(tag string, v reflect.Value) error {
		c, err := column.ForKind(v.Kind())
		if err != nil {
			fmt.Printf("Field [%s] has unsupported column type: %s\n", tag, v.Kind())
			return errContinue
		}

		return col.CreateColumn(tag, c)
	})
}

// InsertStruct converts a given struct to a row
// Fields must be tagged with `col`
func InsertStruct[T any](input *T, structTag string) func(r column.Row) error {
	return func(r column.Row) error {
		return ExecWhenSettable(input, structTag, func(tag string, v reflect.Value) error {
			if err := SetValue(r, v, tag); err != nil {
				return err
			}
			return nil
		})
	}
}

// ExecWhenSettable executes a function on a reflect.Value when it's settable
func ExecWhenSettable[T any](input *T, structTag string, f func(string, reflect.Value) error) error {
	e := reflect.ValueOf(input).Elem()

	for i := 0; i < e.NumField(); i++ {
		field := e.Type().Field(i)
		tag := field.Tag.Get(structTag)
		if strings.TrimSpace(tag) == "" {
			continue
		}

		if e.Field(i).CanSet() {
			err := f(tag, e.Field(i))
			if err != nil {
				if errors.Is(err, errContinue) {
					continue
				}

				return err
			}
		}
	}

	return nil
}

// GetValue attempts to get underlying of given reflect.Value
func GetValue(val reflect.Value) (any, error) {
	switch kind := val.Kind(); kind {
	case reflect.Float32:
		return float32(val.Float()), nil
	case reflect.Float64:
		return val.Float(), nil
	case reflect.Int:
		return int(val.Int()), nil
	case reflect.Int16:
		return int16(val.Int()), nil
	case reflect.Int32:
		return int32(val.Int()), nil
	case reflect.Int64:
		return val.Int(), nil
	case reflect.Uint:
		return uint(val.Uint()), nil
	case reflect.Uint16:
		return uint16(val.Uint()), nil
	case reflect.Uint32:
		return uint32(val.Uint()), nil
	case reflect.Uint64:
		return val.Uint(), nil
	case reflect.Bool:
		return val.Bool(), nil
	case reflect.String:
		return val.String(), nil
	default:
		return nil, fmt.Errorf("column: unsupported column kind (%v)", kind)
	}
}

// SetValue creates a row for a specified reflect.Value
func SetValue(r column.Row, val reflect.Value, colName string) error {
	switch kind := val.Kind(); kind {
	case reflect.Float32:
		r.SetFloat32(colName, float32(val.Float()))
	case reflect.Float64:
		r.SetFloat64(colName, val.Float())
	case reflect.Int:
		r.SetInt(colName, int(val.Int()))
	case reflect.Int16:
		r.SetInt16(colName, int16(val.Int()))
	case reflect.Int32:
		r.SetInt32(colName, int32(val.Int()))
	case reflect.Int64:
		r.SetInt64(colName, val.Int())
	case reflect.Uint:
		r.SetUint(colName, uint(val.Uint()))
	case reflect.Uint16:
		r.SetUint16(colName, uint16(val.Uint()))
	case reflect.Uint32:
		r.SetUint32(colName, uint32(val.Uint()))
	case reflect.Uint64:
		r.SetUint64(colName, val.Uint())
	case reflect.Bool:
		r.SetBool(colName, val.Bool())
	case reflect.String:
		r.SetString(colName, val.String())
	default:
		return fmt.Errorf("column: unsupported column kind (%v)", kind)
	}

	return nil
}
