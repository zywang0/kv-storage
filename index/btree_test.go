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
