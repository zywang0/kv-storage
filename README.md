## KV Storage

This project is based on the key/value storage engine designed by
basho's [bitcask paper](https://riak.com/assets/bitcask-intro.pdf).

<!-- TOC -->

* [Database Startup Process](#database-startup-process)
* [Design](#design)
    * [Bitcask Model Design](#bitcask-model-design)
    * [Memory Design: How data is stored](#memory-design-how-data-is-stored)
        * [Index(Data structure)](#indexdata-structure)
    * [Disk Design: How data is organized](#disk-design-how-data-is-organized)
        * [Record](#record)
    * [I/O Types](#io-types)

<!-- TOC -->

### Database Startup Process

- Step 1: Load the files in the data directory and open their file descriptors.
- Step 2: Traverse the contents of the data file to construct an in-memory index.

### Design

#### Bitcask Model Design

- All keys are stored in memory; all values are stored on disk.
- Bitcask model contains only one read/write(active) file and multiple read-only(inactive) files.
- Merge operation

#### Memory Design: How data is stored

Key: key | Value: ValuePos {Fid, Offset, Size}

##### Index(Data structure)

We used B Tree and Adaptive Radix Tree as our memory index in this project because these two data structures support
efficient insertion, reading, and deletion of data.

#### Disk Design: How data is organized

##### Record

One datafile contains multiple records. We separate our single encoded record into two parts: Header and Real Data.

| crc      | type       | keySize                             | valueSize                           | key                   | value                 |
|----------|------------|-------------------------------------|-------------------------------------|-----------------------|-----------------------|
| uint32   | RecordType | uint32                              | uint32                              | []byte                | []byte                |
| 4  bytes | 1 bytes    | variable-length field (max 5 bytes) | variable-length field (max 5 bytes) | variable-length field | variable-length field |

#### I/O Types

Standard File I/O, MMAP I/O