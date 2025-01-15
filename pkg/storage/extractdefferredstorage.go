package storage

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/ipfs/go-cid"
	carv2 "github.com/ipld/go-car/v2"
	carstorage "github.com/ipld/go-car/v2/storage"
)

var _ ReadableWritableStorage = (*ExtractStorageCar)(nil)

// ExtractStorageCar is a wrapper around
// github.com/ipld/go-car/v2/storage.StorageCar that defers creating the CAR
// until the first Put() operation. In this way it can be optimistically
// instantiated and no file will be created if it is never written to (such as
// in the case of an error).
type ExtractStorageCar struct {
	buffer  *StreamBuffer
	root    cid.Cid

	lk     *sync.RWMutex
	closed bool
	rw     *carstorage.StorageCar

	// onPutOnce ensures the callback runs only once
	onPutOnce sync.Once
	// onPutFunc stores the callback function to run before the first Put
	onPutFunc func()
}

// NewExtractStorageCar creates a new ExtractStorageCar.
func NewExtractStorageCar(writer io.Writer, root cid.Cid) *ExtractStorageCar {
	lk := &sync.RWMutex{}
	return &ExtractStorageCar{
		buffer: NewStreamBuffer(writer),
		root:   root,
		lk:     lk,
	}
}

// Close will clean up any temporary resources used by the storage.
func (dcs *ExtractStorageCar) Close() error {
	dcs.lk.Lock()
	defer dcs.lk.Unlock()

	if dcs.closed {
		return nil
	}
	dcs.closed = true

	// Clear the buffer and signal EOF
	if dcs.buffer != nil {
		dcs.buffer.Close()
	}

	return nil
}

// Has returns true if the underlying CARv1 has the key.
func (dcs *ExtractStorageCar) Has(ctx context.Context, key string) (bool, error) {
	dcs.lk.Lock()
	defer dcs.lk.Unlock()

	if dcs.rw == nil { // not initialised, so we certainly don't have it
		return false, nil
	}

	if rw, err := dcs.readWrite(); err != nil {
		return false, err
	} else {
		return rw.Has(ctx, key)
	}
}

// Get returns data from the underlying CARv1.
func (dcs *ExtractStorageCar) Get(ctx context.Context, key string) ([]byte, error) {
	if digest, ok, err := AsIdentity(key); ok {
		return digest, nil
	} else if err != nil {
		return nil, err
	}

	dcs.lk.Lock()
	defer dcs.lk.Unlock()

	if dcs.rw == nil { // not initialised, so we certainly don't have it
		keyCid, err := cid.Cast([]byte(key))
		if err != nil {
			return nil, fmt.Errorf("bad CID key: %w", err)
		}
		return nil, carstorage.ErrNotFound{Cid: keyCid}
	}

	if rw, err := dcs.readWrite(); err != nil {
		return nil, err
	} else {
		return rw.Get(ctx, key)
	}
}

// GetStream returns data from the underlying CARv1.
func (dcs *ExtractStorageCar) GetStream(ctx context.Context, key string) (io.ReadCloser, error) {
	if digest, ok, err := AsIdentity(key); ok {
		return io.NopCloser(bytes.NewReader(digest)), nil
	} else if err != nil {
		return nil, err
	}

	dcs.lk.Lock()
	defer dcs.lk.Unlock()

	keyCid, err := cid.Cast([]byte(key))
	if err != nil {
		return nil, fmt.Errorf("bad CID key: %w", err)
	}
	if dcs.rw == nil { // not initialised, so we certainly don't have it
		return nil, carstorage.ErrNotFound{Cid: keyCid}
	}

	if rw, err := dcs.readWrite(); err != nil {
		return nil, err
	} else {
		return rw.GetStream(ctx, key)
	}
}

// Put writes data to the underlying CARv1 which will be initialised on the
// first call to Put.
func (dcs *ExtractStorageCar) Put(ctx context.Context, key string, data []byte) error {
	// Execute the OnPut callback if it's the first Put
	dcs.onPutOnce.Do(func() {
		dcs.lk.RLock()
		cb := dcs.onPutFunc
		dcs.lk.RUnlock()
		if cb != nil {
			cb()
		}
	})
	if _, ok, err := AsIdentity(key); ok {
		return nil
	} else if err != nil {
		return err
	}

	dcs.lk.Lock()
	defer dcs.lk.Unlock()

	if rw, err := dcs.readWrite(); err != nil {
		return err
	} else {
		err = rw.Put(ctx, key, data)
		return err
	}
}

// readWrite returns a ReadableWritableStorage which is lazily initialised. It
// is not synchronized so calls that need thread safety should be wrapped in a
// mutex. This can be used to directly access the underlying CARv1 and cause it
// to be initialised.
func (dcs *ExtractStorageCar) readWrite() (ReadableWritableStorage, error) {
	if dcs.closed {
		return nil, errClosed
	}
	if dcs.rw == nil {
		rw, err := carstorage.NewReadableWritable(
			dcs.buffer,
			[]cid.Cid{dcs.root},
			carv2.WriteAsCarV1(true),
			carv2.StoreIdentityCIDs(false),
			carv2.UseWholeCIDs(false),
		)
		if err != nil {
			return nil, err
		}
		dcs.rw = rw
	}
	return dcs.rw, nil
}

// OnPut registers a callback to be executed once before the first Put operation
func (dcs *ExtractStorageCar) OnPut(cb func()) {
	if cb == nil {
		return
	}
	dcs.lk.Lock()
	defer dcs.lk.Unlock()
	if dcs.onPutFunc == nil {
		dcs.onPutFunc = cb
	} 
}


// StreamBuffer implements ReaderAtWriterAt interface using a bytes.Buffer
type StreamBuffer struct {
	buf    *bytes.Buffer
	reader io.Writer
	closed bool
	lk     sync.RWMutex
}

func NewStreamBuffer(writer io.Writer) *StreamBuffer {
	return &StreamBuffer{
		buf:    bytes.NewBuffer(nil),
		reader: writer,
	}
}

func (sb *StreamBuffer) Close() {
	sb.lk.Lock()
	defer sb.lk.Unlock()
	sb.closed = true
	// Clear the buffer
	sb.buf.Reset()
}

func (sb *StreamBuffer) Write(p []byte) (n int, err error) {
	sb.lk.RLock()
	if sb.closed {
		sb.lk.RUnlock()
		return 0, io.EOF
	}
	sb.lk.RUnlock()
	
	n, err = sb.buf.Write(p)
	if err != nil {
		return n, err
	}
	// Also write to the output writer
	return sb.reader.Write(p)
}

func (sb *StreamBuffer) WriteAt(p []byte, off int64) (n int, err error) {
	sb.lk.RLock()
	if sb.closed {
		sb.lk.RUnlock()
		return 0, io.EOF
	}
	sb.lk.RUnlock()
	
	return sb.Write(p)
}

func (sb *StreamBuffer) ReadAt(p []byte, off int64) (n int, err error) {
	sb.lk.RLock()
	defer sb.lk.RUnlock()
	
	if sb.closed {
		return 0, io.EOF
	}
	
	data := sb.buf.Bytes()
	if off >= int64(len(data)) {
		return 0, io.EOF
	}
	n = copy(p, data[off:])
	return n, nil
}