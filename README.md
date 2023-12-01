## KV Storage Implementation

### Design

#### Bitcask Model Design

When the storage space of the logfile is larger than the threshold, a new logfile file is generated as the active file,
and the old logfile is marked as inactive. Therefore, there is only one active logfile in the storage engine of the
bitcask model, while there are multiple inactive logfiles.

#### Memory Design: How data is stored

##### Index(Data structure):

Supports efficient insertion, reading, and deletion of data.

Efficient traversal, then preferably choose ordered and naturally supported data structures

--> Option: B Tree, B+ Tree, Skip list, Red-black tree.

> B+ Tree: All data will appear in the leaf nodes, which will form a bidirectional LinkedList.

Key: key | Value: ValuePos {Fid, Offset, Size}

#### Disk Design: How data is organized

##### I/O Types

Standard I/O, MMAP I/O

#### Database Startup Process

- Step 1: Load the files in the data directory and open their file descriptors.
- Step 2: Traverse the contents of the data file to construct an in-memory index.