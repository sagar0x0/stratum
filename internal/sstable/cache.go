package sstable

import (
	"bytes"
	"container/list"
	"sync"
)

const NumShards = 4

// BlockCache is a sharded LRU cache for SSTable data blocks.
type BlockCache struct {
	shards   [NumShards]*cacheShard
	capacity int64
}

type cacheItem struct {
	fileID uint64
	offset uint64
	data   []byte
}

type cacheShard struct {
	mu    sync.Mutex
	items map[uint64]map[uint64]*list.Element
	order *list.List
	size  int64
	cap   int64
}

// NewBlockCache creates a new sharded LRU block cache.
func NewBlockCache(capacityBytes int64) *BlockCache {
	c := &BlockCache{
		capacity: capacityBytes,
	}
	shardCap := capacityBytes / NumShards
	if shardCap < 1 {
		shardCap = 1
	}

	for i := 0; i < NumShards; i++ {
		c.shards[i] = &cacheShard{
			items: make(map[uint64]map[uint64]*list.Element),
			order: list.New(),
			cap:   shardCap,
		}
	}
	return c
}

func (c *BlockCache) getShard(fileID, offset uint64) *cacheShard {
	// A simple hash function to distribute blocks across shards
	hash := fileID ^ (offset * 2654435761)
	return c.shards[hash%NumShards]
}

// Get retrieves a block from the cache.
func (c *BlockCache) Get(fileID uint64, offset uint64) ([]byte, bool) {
	shard := c.getShard(fileID, offset)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	if fileMap, ok := shard.items[fileID]; ok {
		if elem, ok := fileMap[offset]; ok {
			shard.order.MoveToFront(elem)
			return elem.Value.(*cacheItem).data, true
		}
	}
	return nil, false
}

// Put adds a block to the cache.
func (c *BlockCache) Put(fileID uint64, offset uint64, data []byte) {
	shard := c.getShard(fileID, offset)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	fileMap, ok := shard.items[fileID]
	if !ok {
		fileMap = make(map[uint64]*list.Element)
		shard.items[fileID] = fileMap
	}

	if elem, ok := fileMap[offset]; ok {
		// Update existing
		oldSize := len(elem.Value.(*cacheItem).data)
		elem.Value.(*cacheItem).data = data
		shard.size += int64(len(data) - oldSize)
		shard.order.MoveToFront(elem)
	} else {
		// Add new
		item := &cacheItem{fileID: fileID, offset: offset, data: data}
		elem := shard.order.PushFront(item)
		fileMap[offset] = elem
		shard.size += int64(len(data))
	}

	// Evict if over capacity
	for shard.size > shard.cap && shard.order.Len() > 0 {
		back := shard.order.Back()
		shard.order.Remove(back)
		evictedItem := back.Value.(*cacheItem)
		shard.size -= int64(len(evictedItem.data))
		delete(shard.items[evictedItem.fileID], evictedItem.offset)
		if len(shard.items[evictedItem.fileID]) == 0 {
			delete(shard.items, evictedItem.fileID)
		}
	}
}

// RemoveFile removes all blocks associated with a file ID from the cache.
func (c *BlockCache) RemoveFile(fileID uint64) {
	for i := 0; i < NumShards; i++ {
		shard := c.shards[i]
		shard.mu.Lock()
		if fileMap, ok := shard.items[fileID]; ok {
			for _, elem := range fileMap {
				shard.order.Remove(elem)
				evictedItem := elem.Value.(*cacheItem)
				shard.size -= int64(len(evictedItem.data))
			}
			delete(shard.items, fileID)
		}
		shard.mu.Unlock()
	}
}

// Compare returns an integer comparing two byte slices lexicographically.
// The result will be 0 if a==b, -1 if a < b, and +1 if a > b.
func Compare(a, b []byte) int {
	return bytes.Compare(a, b)
}
