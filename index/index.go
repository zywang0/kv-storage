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
	// Size returns the number of entries in the index
	Size() int
	// Iterator returns an iterator for the index
	Iterator(reverse bool) Iterator
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
		// TODO: Adaptive Radix Tree Index
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

// Iterator General index iterator
type Iterator interface {
	// Rewind resets the iterator to the beginning, i.e., the first entry
	Rewind()

	// Seek seeks to a target key that is >= or <= the given key
	Seek(key []byte)

	// Next moves to the next key.
	Next()

	// Valid returns whether the iterator is still valid, i.e., if all keys have been traversed
	Valid() bool

	// Key returns the key at the current iterator position.
	Key() []byte

	// Value returns the value (position information) at the current iterator position.
	Value() *data.LogRecordPos

	// Close closes the iterator and releases any resources.
	Close()
}
