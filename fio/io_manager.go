package fio

/*
In the context of file permissions, 0644(octal number) typically means:
            rwx
6 (which is 110 in binary) for the owner: read (4) and write (2) permissions, but no execute permission.
4 (which is 100 in binary) for the group: read permission, but no write or execute permissions.
4 (which is 100 in binary) for others: same as for the group.
*/

// DataFilePerm read and write for the owner, read-only for others
const DataFilePerm = 0644

type FileIOType = byte

const (
	StandardFIO FileIOType = iota
	MemoryMap
)

type IOManager interface {
	Read([]byte, int64) (int, error)

	Write([]byte) (int, error)

	Sync() error

	Close() error

	Size() (int64, error)
}

func NewIOManager(fileName string, ioType FileIOType) (IOManager, error) {
	switch ioType {
	case StandardFIO:
		return NewFileIOManager(fileName)
	case MemoryMap:
		return NewMMapIOManager(fileName)
	default:
		panic("unsupported io type")
	}
}
