# rdb
A database written in rust

# Design

## Storage
### Page Cache (Buffer manager)
LRU cache to manage the pages in memory. When a page gets evicted, it will ask the storage layer to flush the page.
This also supports flushing all pages (used for periodic flushing).

### File Manager
Directly interacts with the file system. When the page manager requests a page from storage, the file manager will
handle opening, reading, and writing to the file.

## Disk Representation
### B+ Tree
- Node types 
  - Inner Node
    - Number of max keys in a node depends on key size.
      - Won't vary in a tree, can be part of tree metadata.
  - Leaf Node
    - Number of max key value pairs will vary depending on the size of the key + value.
    - primary tree will have all tuples, index trees will only have the index and primary key.
    - Layout: | header | tuple1 | tuple2 | tuple3 |
    - Header layout: | previous | next | tuple count | 
    - Tuple layout: | column1 | column2 | ... | columnN |
    ```
    Layout
    |-----------------------------------------------------------------|
    | |-Header-||-Leaf Header-----||-Tuple0----------------------|    |
    | |  size  || previous | next || column0 | column1 | column2 |    |
    | |--------||-----------------||-----------------------------|    |
    | |-Tuple1----------------------||-Tuple2----------------------|  |
    | | column0 | column1 | column2 || column0 | column1 | column2 |  |
    | |-----------------------------||-----------------------------|  |
    |-----------------------------------------------------------------|
    ```
- node buffer is a full page

### KV Store
A key-value store for system metadata. Supports
- insert/update value
  - Uses an append-only process. The store will use the newest object in the blob.
  - Eventually this should have a compaction process to clean up the old objects.
- get value by key

## Tables
### System Defined
- Holds metadata about the system.
- Will be queryable but will not allow mutable statements.
- Queries will be recursive
  - all queries will reach out to the system table to get metadata
  - system metadata will be hardcoded as the base case.

#### Incomplete breakdown of what we need to store
```yaml
users:
  columns:
    id: int
    username: string (20)
    firstName: string (20)
    lastName: string (20)
  primaryKeys:
    - id
```
### User Defined
#### Example table
| id | username | firstName | lastName |
| - | - | - | - |
| 1 | ironman | Tony | Stark |
| 2 | hulk | Bruce | Banner |

## TODO:
- [x] File manager
- [x] Page manager/cache
  - [x] read from and write to cache
  - [x] parallel read/write
  - [x] callback on cache eviction