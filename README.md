# rdb
A database written in rust

# Design
## Page Cache (Buffer manager)
LRU cache to manage the pages in memory. When a page gets evicted, it will ask the storage layer to flush the page.

## File Manager
Directly interacts with the file system. When the page manager requests a page from storage, the file manager will
handle opening, reading, and writing to the file.

## TODO:
- [x] File manager
- [x] Page manager/cache
  - [x] read from and write to cache
  - [x] parallel read/write
  - [x] callback on cache eviction