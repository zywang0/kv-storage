package index

import (
	"github.com/google/btree"
	"kv-project/data"
	"sync"
)

type BTree struct {
	// For implemented Btree, write operations are not safe for concurrent mutation but Read operations are.
	tree *btree.BTree
	lock *sync.RWMutex
}

func NewBtree() *BTree {
	return &BTree{tree: btree.New(32), lock: new(sync.RWMutex)}
}

func (bt *BTree) Put(key []byte, pos *data.LogRecordPos) bool {
	bt.lock.Lock()
	bt.tree.ReplaceOrInsert(&Item{key: key, pos: pos})
	bt.lock.Unlock()
	return true
}
func (bt *BTree) Get(key []byte) *data.LogRecordPos {
	item := bt.tree.Get(&Item{key: key})
	if item == nil {
		return nil
	}
	return item.(*Item).pos
}
func (bt *BTree) Delete(key []byte) bool {
	bt.lock.Lock()
	item := bt.tree.Delete(&Item{key: key})
	bt.lock.Unlock()
	if item == nil {
		return false
	}
	return true
}
