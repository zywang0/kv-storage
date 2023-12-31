package index

import (
	"github.com/stretchr/testify/assert"
	"kv-project/data"
	"testing"
)

func TestBTree_Put(t *testing.T) {
	btree := NewBtree()

	res1 := btree.Put(nil, &data.LogRecordPos{Fid: 1, Offset: 10})
	assert.True(t, res1)

	res2 := btree.Put([]byte("a"), &data.LogRecordPos{Fid: 1, Offset: 100})
	assert.True(t, res2)
}

func TestBTree_Get(t *testing.T) {
	btree := NewBtree()

	res1 := btree.Put(nil, &data.LogRecordPos{Fid: 1, Offset: 10})
	assert.True(t, res1)

	pos1 := btree.Get(nil)
	assert.Equal(t, uint32(1), pos1.Fid)
	assert.Equal(t, int64(10), pos1.Offset)

	res2 := btree.Put([]byte("a"), &data.LogRecordPos{Fid: 1, Offset: 100})
	assert.True(t, res2)

	pos2 := btree.Get([]byte("a"))
	assert.Equal(t, uint32(1), pos2.Fid)
	assert.Equal(t, int64(100), pos2.Offset)
}

func TestBTree_Delete(t *testing.T) {
	btree := NewBtree()

	res1 := btree.Put(nil, &data.LogRecordPos{Fid: 1, Offset: 10})
	assert.True(t, res1)
	res2 := btree.Delete(nil)
	assert.True(t, res2)

	res3 := btree.Put([]byte("a"), &data.LogRecordPos{Fid: 1, Offset: 100})
	assert.True(t, res3)
	res4 := btree.Delete([]byte("a"))
	assert.True(t, res4)

}

func TestBTree_Iterator(t *testing.T) {
	btree := NewBtree()

	iterator1 := btree.Iterator(false)
	assert.Equal(t, false, iterator1.Valid())

	btree.Put([]byte("a"), &data.LogRecordPos{Fid: 1, Offset: 100})
	iterator2 := btree.Iterator(false)
	assert.Equal(t, true, iterator2.Valid())

	iterator2.Next()
	assert.Equal(t, false, iterator2.Valid())

	btree.Put([]byte("b"), &data.LogRecordPos{Fid: 1, Offset: 100})
	btree.Put([]byte("c"), &data.LogRecordPos{Fid: 1, Offset: 100})
	btree.Put([]byte("d"), &data.LogRecordPos{Fid: 1, Offset: 100})
	iterator3 := btree.Iterator(false)
	for iterator3.Rewind(); iterator3.Valid(); iterator3.Next() {
		assert.NotNil(t, iterator3.Key())
	}

	iterator4 := btree.Iterator(true)
	for iterator4.Rewind(); iterator4.Valid(); iterator4.Next() {
		assert.NotNil(t, iterator4.Key())
	}

	iterator5 := btree.Iterator(false)
	iterator5.Seek([]byte("c"))
	assert.NotNil(t, iterator5.Key())
}
