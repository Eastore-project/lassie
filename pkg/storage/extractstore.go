package storage

import (
	"context"
	"io"
	"sync"

	"github.com/ipfs/go-cid"
	"github.com/ipld/go-car/v2"
	carstorage "github.com/ipld/go-car/v2/storage"
	"github.com/ipld/go-ipld-prime/storage"
)

var _ storage.ReadableStorage = (*StdinReadStorage)(nil)

type StdinReadStorage struct {
	blocks map[string][]byte
	done   bool
	lk     *sync.RWMutex
	cond   *sync.Cond
}

func NewStdinReadStorage(ctx context.Context, reader io.Reader) (*StdinReadStorage, error) {
	var lk sync.RWMutex
	srs := &StdinReadStorage{
		blocks: make(map[string][]byte),
		lk:     &lk,
		cond:   sync.NewCond(&lk),
	}
	rdr, err := car.NewBlockReader(reader)
	if err != nil {
		return nil, err
	}
	go func() {
		for {
			blk, err := rdr.Next()
			if err == io.EOF {
				srs.lk.Lock()
				srs.cond.Broadcast()
				srs.done = true
				srs.cond.Broadcast()
				srs.lk.Unlock()
				return
			}
			if err != nil {
				panic(err)
			}
			srs.lk.Lock()
			srs.blocks[string(blk.Cid().Hash())] = blk.RawData()
			srs.cond.Broadcast()
			srs.lk.Unlock()
		}
	}()
	return srs, nil
}

func (srs *StdinReadStorage) Has(ctx context.Context, key string) (bool, error) {
	_, err := srs.Get(ctx, key)
	if err != nil {
		return false, err
	}
	return true, nil
}
func (srs *StdinReadStorage) Get(ctx context.Context, key string) ([]byte, error) {
    c, err := cid.Cast([]byte(key))
    if err != nil {
        return nil, err
    }
    srs.lk.Lock()
    defer srs.lk.Unlock()
    for {
		if srs.done {
			return nil, carstorage.ErrNotFound{Cid: c}
		}
        if data, ok := srs.blocks[string(c.Hash())]; ok {
			srs.blocks[string(c.Hash())] = nil
            return data, nil
        }
        srs.cond.Wait()
    }
}