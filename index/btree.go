package index

import (
	"bytes"
	"github.com/google/btree"
	"kv-project/data"
	"sort"
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
func (bt *BTree) Size() int {
	return bt.tree.Len()
}
func (bt *BTree) Iterator(reverse bool) Iterator {
	if bt.tree == nil {
		return nil
	}
	bt.lock.RLock()
	defer bt.lock.RUnlock()
	return newBTreeIterator(bt.tree, reverse)
}

// BTreeIterator represents an iterator for BTree index.
type BTreeIterator struct {
	currIndex int     // Current index position during iteration
	reverse   bool    // Whether it is a reverse iteration
	values    []*Item // Key + position index information
}

func newBTreeIterator(tree *btree.BTree, reverse bool) *BTreeIterator {
	var idx int
	values := make([]*Item, tree.Len())

	// Store all data in an array
	saveValues := func(item btree.Item) bool {
		values[idx] = item.(*Item)
		idx++
		return true
	}
	if reverse {
		tree.Descend(saveValues)
	} else {
		tree.Ascend(saveValues)
	}

	return &BTreeIterator{
		currIndex: 0,
		reverse:   reverse,
		values:    values,
	}
}

func (bti *BTreeIterator) Rewind() {
	bti.currIndex = 0
}
func (bti *BTreeIterator) Seek(key []byte) {
	if bti.reverse {
		bti.currIndex = sort.Search(len(bti.values), func(i int) bool {
			return bytes.Compare(bti.values[i].key, key) <= 0
		})
	} else {
		bti.currIndex = sort.Search(len(bti.values), func(i int) bool {
			return bytes.Compare(bti.values[i].key, key) >= 0
		})
	}
}
func (bti *BTreeIterator) Next() {
	bti.currIndex += 1
}

func (bti *BTreeIterator) Valid() bool {
	return bti.currIndex < len(bti.values)
}

func (bti *BTreeIterator) Key() []byte {
	return bti.values[bti.currIndex].key
}

func (bti *BTreeIterator) Value() *data.LogRecordPos {
	return bti.values[bti.currIndex].pos
}

func (bti *BTreeIterator) Close() {
	bti.values = nil
}
