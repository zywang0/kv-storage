package fio

import (
	"github.com/stretchr/testify/assert"
	"os"
	"path/filepath"
	"testing"
)

func destroyTempFile(name string) {
	if err := os.RemoveAll(name); err != nil {
		panic(err)
	}
}

func TestNewFileIOManager(t *testing.T) {
	path := filepath.Join("/tmp", "a.data")
	fileIOManager, err := NewFileIOManager(path)
	defer destroyTempFile(path)

	assert.Nil(t, err)
	assert.NotNil(t, fileIOManager)
}

func TestFileIO_Write(t *testing.T) {
	path := filepath.Join("/tmp", "kv_project.data")
	fileIOManager, err := NewFileIOManager(path)
	defer destroyTempFile(path)

	assert.Nil(t, err)
	assert.NotNil(t, fileIOManager)

	write, err := fileIOManager.Write([]byte(""))
	assert.Equal(t, 0, write)
	assert.Nil(t, err)

	write, err = fileIOManager.Write([]byte("Hello"))
	assert.Equal(t, 5, write)
	assert.Nil(t, err)
}

func TestFileIO_Read(t *testing.T) {
	path := filepath.Join("/tmp", "kv_project.data")
	fileIOManager, err := NewFileIOManager(path)

	assert.Nil(t, err)
	assert.NotNil(t, fileIOManager)

	write, err := fileIOManager.Write([]byte("Hello"))
	assert.Equal(t, 5, write)
	assert.Nil(t, err)

	bytes := make([]byte, 5)
	read, err := fileIOManager.Read(bytes, 0)
	assert.Equal(t, 5, read)
	assert.Equal(t, []byte("Hello"), bytes)
}

func TestFileIO_Sync(t *testing.T) {
	path := filepath.Join("/tmp", "kv_project.data")
	fileIOManager, err := NewFileIOManager(path)
	defer destroyTempFile(path)

	assert.Nil(t, err)
	assert.NotNil(t, fileIOManager)

	err = fileIOManager.Sync()
	assert.Nil(t, err)
}

func TestFileIO_Close(t *testing.T) {
	path := filepath.Join("/tmp", "kv_project.data")
	fileIOManager, err := NewFileIOManager(path)
	defer destroyTempFile(path)

	assert.Nil(t, err)
	assert.NotNil(t, fileIOManager)

	err = fileIOManager.Close()
	assert.Nil(t, err)
}
