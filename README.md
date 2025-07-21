[![Build Status](https://github.com/aergoio/kv3/actions/workflows/ci.yml/badge.svg)](https://github.com/aergoio/kv3/actions/workflows/ci.yml)

# kv3

A high-performance embedded key-value database with a radix trie index structure

## Overview

kv3 is a persistent key-value store designed for high performance and reliability. It uses a radix trie for indexing keys.

## Features

- **Entire values**: Values are stored continuously, without partition or chunking
- **Radix trie indexing**: Fast key lookups with O(n) complexity
- **ACID transactions**: Support for atomic operations with commit/rollback
- **Write-Ahead Logging (WAL)**: Ensures durability and crash recovery
- **Configurable write modes**: Balance between performance and durability
- **Efficient page cache**: Minimizes disk I/O with intelligent caching
- **Concurrent access**: Thread-safe operations with appropriate locking

## Architecture

kv3 databases consist of three files:

1. **Values file**: Stores values continuously. Reuse space when a value is deleted
2. **Keys file**: Contains the radix trie structure to store keys
3. **WAL file**: Contains flushed values and index pages (single WAL file for the 2 files above)

### Radix Trie Structure

The index uses a radix trie (also known as a patricia trie) to efficiently locate keys:

- Each node in the trie represents a part of the key
- The trie is organized by byte values (0-255)
- Leaf pages store entries with the same prefix
- When a leaf page becomes full, it's converted to a radix page

### Implementation Details

The database uses a page size of 4KB

There are 2 types of pages (on the keys/index file):

1. **Radix Pages**: Internal nodes of the trie, containing pointers to other pages
2. **Leaf Pages**: Terminal nodes containing key suffixes and pointers to values

Each radix page contains up to 3 sub-pages. Each sub-page has a map of byte 0-255 to another sub-page (can be either radix or leaf sub-page)

Each leaf page can contain as many sub-pages that fit on it

When a new key is inserted on a sub-page:

1. If it still fits on the leaf page, keep it there
2. If it fits on a new leaf page, move it to one
3. Otherwise, convert the leaf sub-page into a radix sub-page

The conversion means that the first byte of every key will be used as a new map to other sub-pages

The database grows smoothly. When a page becomes full, it generally allocates just another one

Using the default config, the engine uses a worker thread to flush changes to disk

- Keys are limited to 2KB
- Values are limited to 128MB (this limit can be removed)

## Usage

### Basic Operations

```go
// Open or create a database
db, err := kv3.Open("path/to/database")
if err != nil {
    // Handle error
}
defer db.Close()

// Set a key-value pair
err = db.Set([]byte("key"), []byte("value"))

// Get a value
value, err := db.Get([]byte("key"))

// Delete a key
err = db.Delete([]byte("key"))
```

### Transactions

```go
// Begin a transaction
tx, err := db.Begin()
if err != nil {
    // Handle error
}

// Perform operations within the transaction
err = tx.Set([]byte("key1"), []byte("value1"))
err = tx.Set([]byte("key2"), []byte("value2"))

// Commit or rollback
if everythingOk {
    err = tx.Commit()
} else {
    err = tx.Rollback()
}
```

### Configuration Options

```go
options := kv3.Options{
    "ReadOnly": true,                    // Open in read-only mode
    "CacheSizeThreshold": 10000,         // Maximum number of pages in cache
    "DirtyPageThreshold": 5000,          // Maximum dirty pages before flush
    "CheckpointThreshold": 10 * 1024 * 1024,  // WAL size before checkpoint (10MB)
}

db, err := kv3.Open("path/to/database", options)
```

## Performance Considerations

- **Write Modes**: Choose between durability and performance
  - `CallerThread_WAL_Sync`: Maximum durability, lowest performance
  - `CallerThread_WAL`: High durability, low performance
  - `WorkerThread_WAL`: Good performance, low durability (default)
  - `WorkerThread_NoWAL_NoSync`: Maximum performance, lowest durability

- **Cache Size**: Adjust based on available memory and workload
  - Larger cache improves read performance but uses more memory

- **Checkpoint Threshold**: Controls WAL file size before checkpoint
  - Smaller values reduce recovery time but may impact performance

With the default option, a transaction that was just committed can be lost on a crash or power loss (if the worker thread did not flush it yet)

For higher durability, make the engine write to disk on each commit by selecting the `CallerThread_WAL` write mode

## Limitations

- Single connection per database: Only one process can open the database at a time
- Concurrent access model: Supports one writer thread or multiple reader threads simultaneously

## Performance

Here is a comparison with popupar dbs:

| Metric | LevelDB | RocksDB | BadgerDB | ForestDB | kv3 |
|--------|---------|---------|----------|----------|-----|
| Set 2M values | 2m 44.45s | 19.53s | 13.81s | 18.95s | 9.82s |
| 20K txns (10 items each) | 1m 0.09s | 1m 4.86s | 1.32s | 2.78s | 1.89s |
| Space after write | 1052.08 MB | 1501.43 MB | 2002.38 MB | 1715.76 MB | 0.01 MB |
| Space after close | 1158.78 MB | 1189.29 MB | 1203.11 MB | 2223.16 MB | 1730.73 MB |
| Read 2M values (fresh) | 1m 26.87s | 8.87s | 35.14s | 17.21s | 5.72s |
| Read 2M values (cold) | 1m 34.46s | 1m 29.4s | 38.36s | 16.84s | 10.55s |

Benchmark writting 2 million records (key: 33 random bytes, value: 750 random bytes) in a single transaction and then reading them in non-sequential order

Also insertion using 20 thousand transactions

The fastest on reads is KV3

The fastest on writes is BadgerDB

Note: LevelDB, RocksDB and BadgerDB can be made faster by disabling data compression

## License

Apache 2.0
