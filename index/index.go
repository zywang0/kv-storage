package index

import (
	"bytes"
	"github.com/google/btree"
	"kv-project/data"
)

// Indexer General index interface
type Indexer interface {
	Put(key []byte, pos *data.LogRecordPos) bool
	Get(key []byte) *data.LogRecordPos
	Delete(key []byte) bool
}

type IndexerType = int8

const (
	Btree IndexerType = iota + 1
	ART
)

func NewIndexer(tp IndexerType) Indexer {
	switch tp {
	case Btree:
		return NewBtree()
	case ART:
		//TODO
		return nil
	default:
		panic("unsupported index type")
	}
}

type Item struct {
	key []byte
	pos *data.LogRecordPos
}

func (ai *Item) Less(bi btree.Item) bool {
	return bytes.Compare(ai.key, bi.(*Item).key) == -1
}
