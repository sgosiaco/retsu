package retsu

import (
	"bytes"
	"encoding/gob"
	"io"
	"os"

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
