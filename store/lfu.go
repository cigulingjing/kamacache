package store

import (
	"container/list"
	"math"
	"sync"
	"time"
)

// last frequency cache, replace least frequently used
type lfucache struct {
	mu              sync.RWMutex
	keyMap          map[string]*list.Element // list element map
	freqListMap     map[int]*list.List       // frequency map ( frequency -> entry list)
	minFreq         int
	expireMap       map[string]time.Time // expiration map
	maxBytes        int64                // Max allowed bytes
	usedBytes       int64                // Used bytes
	onEvicted       func(key string, value Value)
	cleanupInterval time.Duration
	cleanupTicker   *time.Ticker
	closeCh         chan struct{} // Used to closed goroutine
	totalLen        int           // Total count of cache items
}

// lfuEntry indicates an entry in the cache
type lfuEntry struct {
	key   string
	freq  int
	value Value
}

// newlfucache create a new LFU cache instance
func newLFUCache(opts Options) *lfucache {
	// Set default cleanup interval
	cleanupInterval := opts.CleanupInterval
	if cleanupInterval <= 0 {
		cleanupInterval = time.Minute
	}

	c := &lfucache{
		keyMap:          make(map[string]*list.Element),
		freqListMap:     make(map[int]*list.List),
		minFreq:         1,
		expireMap:       make(map[string]time.Time),
		maxBytes:        opts.MaxBytes,
		usedBytes:       0,
		onEvicted:       opts.OnEvicted,
		cleanupInterval: cleanupInterval,
		closeCh:         make(chan struct{}),
		totalLen:        0,
	}

	// Create a cleanup routine, regularly clean up expired items
	c.cleanupTicker = time.NewTicker(c.cleanupInterval)
	go c.cleanupLoop()

	return c
}

// addFreq adds the entry to the next frequency list.
// If call this method, you must hold write lock
func (c *lfucache) addFreq(key string) error {

	// Check if the key exists. Maybe it was deleted in the async delete operation
	if elem, ok := c.keyMap[key]; ok {
		entry := elem.Value.(*lfuEntry)
		c.removeItemFromFreqList(elem)
		entry.freq++
		if _, ok := c.freqListMap[entry.freq]; !ok {
			c.freqListMap[entry.freq] = list.New()
		}
		// Push entry to back of the list
		c.freqListMap[entry.freq].PushBack(entry)
		return nil
	} else {
		return ErrCacheNotFound
	}
}

// removeItemFromFreqList removes the item from the frequency list.
// Will update the minFreq if the list is empty
func (c *lfucache) removeItemFromFreqList(elem *list.Element) {
	entry := elem.Value.(*lfuEntry)
	c.freqListMap[entry.freq].Remove(elem)

	if c.freqListMap[entry.freq].Len() == 0 {
		delete(c.freqListMap, entry.freq)
	}

	// Attemp update minFreq
	if entry.freq == c.minFreq && c.freqListMap[c.minFreq] == nil {
		c.minFreq = math.MaxInt
		// TODO The efficiency of the implementation given here is O(n), is it possible to optimise?
		for k := range c.freqListMap {
			if k < c.minFreq {
				c.minFreq = k
			}
		}
	}
}

// Get cache item,if it exists and is not expired, return it. If item is expired, clean up.
func (c *lfucache) Get(key string) (Value, bool) {
	// Read lock
	c.mu.RLock()
	elem, ok := c.keyMap[key]
	if !ok {
		c.mu.RUnlock()
		return nil, false
	}

	// Check if expired
	if expTime, hasExp := c.expireMap[key]; hasExp && time.Now().After(expTime) {
		c.mu.RUnlock()
		// Delete item asychronously to avoid blocking the read lock
		go c.Delete(key)
		return nil, false
	}

	// Get value and release read lock
	entry := elem.Value.(*lfuEntry)
	value := entry.value
	c.mu.RUnlock()

	// Get write lock to update the frequency
	c.mu.Lock()
	c.addFreq(key)
	c.mu.Unlock()

	return value, true
}

// Set add or update cache item
func (c *lfucache) Set(key string, value Value) error {
	return c.SetWithExpiration(key, value, 0)
}

// SetWithExpiration add or update cache item and set expiration time
func (c *lfucache) SetWithExpiration(key string, value Value, expiration time.Duration) error {
	// If value is nil, delete the key
	if value == nil {
		c.Delete(key)
		return nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// Compulte expiration time
	var expTime time.Time
	if expiration > 0 {
		expTime = time.Now().Add(expiration)
		c.expireMap[key] = expTime
	} else {
		delete(c.expireMap, key)
	}

	if elem, ok := c.keyMap[key]; ok {
		// Key already exists
		oldEntry := elem.Value.(*lfuEntry)
		c.usedBytes += int64(value.Len() - oldEntry.value.Len())
		oldEntry.value = value

		return c.addFreq(key)
	} else {
		// Key not exists
		entry := &lfuEntry{key: key, value: value, freq: 1}
		c.usedBytes += int64(len(key) + value.Len())

		// Add to map
		if c.freqListMap[1] == nil {
			c.freqListMap[1] = list.New()
		}
		elem := c.freqListMap[1].PushBack(entry)
		c.keyMap[key] = elem

		// Add new entry to the list
		c.totalLen++
		c.minFreq = 1
	}

	// Clean cache
	c.evict()

	return nil
}

// Delete: remove cache item
func (c *lfucache) Delete(key string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	if elem, ok := c.keyMap[key]; ok {
		c.removeElement(elem)
		return true
	}
	return false
}

// Clear: clear all cache items
func (c *lfucache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	// If set callback function, call it for each item
	if c.onEvicted != nil {
		for _, elem := range c.keyMap {
			entry := elem.Value.(*lfuEntry)
			c.onEvicted(entry.key, entry.value)
		}
	}

	// Initial cache to empty
	c.freqListMap = make(map[int]*list.List)
	c.keyMap = make(map[string]*list.Element)
	c.expireMap = make(map[string]time.Time)
	c.minFreq = 1
	c.totalLen = 0
	c.usedBytes = 0
}

func (c *lfucache) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.totalLen
}

// removeElement remove the element from the cache
// If call this method, you must hold write lock
func (c *lfucache) removeElement(elem *list.Element) {
	entry := elem.Value.(*lfuEntry)

	// Remove from map
	c.removeItemFromFreqList(elem)
	delete(c.keyMap, entry.key)
	delete(c.expireMap, entry.key)
	c.usedBytes -= int64(len(entry.key) + entry.value.Len())
	c.totalLen--

	// Call callback function to delete data source
	if c.onEvicted != nil {
		c.onEvicted(entry.key, entry.value)
	}
}

// evict remove expired and memory limit exceeded items
// If call this method, you must hold write lock
func (c *lfucache) evict() {
	// Expired items
	now := time.Now()
	for key, expTime := range c.expireMap {
		if now.After(expTime) {
			if elem, ok := c.keyMap[key]; ok {
				c.removeElement(elem)
			}
		}
	}

	// Memory exceeded
	for c.maxBytes > 0 && c.usedBytes > c.maxBytes && c.totalLen > 0 {
		// Element at the front of the minFreq list
		elem := c.freqListMap[c.minFreq].Front()
		if elem != nil {
			c.removeElement(elem)
		}
	}
}

// cleanupLoop evict regularly
func (c *lfucache) cleanupLoop() {
	for {
		select {
		case <-c.cleanupTicker.C:
			c.mu.Lock()
			c.evict()
			c.mu.Unlock()
		case <-c.closeCh:
			return
		}
	}
}

// Close stop the cleanup loop and close the cache
func (c *lfucache) Close() {
	if c.cleanupTicker != nil {
		c.cleanupTicker.Stop()
		close(c.closeCh)
	}
}

// GetWithExpiration get value and expiration time
func (c *lfucache) GetWithExpiration(key string) (Value, time.Duration, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	elem, ok := c.keyMap[key]
	if !ok {
		return nil, 0, false
	}

	// Check if expired
	now := time.Now()
	if expTime, hasExp := c.expireMap[key]; hasExp {
		if now.After(expTime) {
			// expired
			return nil, 0, false
		}

		// not expired
		ttl := expTime.Sub(now)
		c.addFreq(key)
		return elem.Value.(*lfuEntry).value, ttl, true
	}

	// no expiration
	c.addFreq(key)
	return elem.Value.(*lfuEntry).value, 0, true
}

// GetExpiration get the expiration time of the key.
// Don't need update frequecy
func (c *lfucache) GetExpiration(key string) (time.Time, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	expTime, ok := c.expireMap[key]
	return expTime, ok
}

// UpdateExpiration update the expiration time of the key
func (c *lfucache) UpdateExpiration(key string, expiration time.Duration) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, ok := c.keyMap[key]; !ok {
		return false
	}

	if expiration > 0 {
		c.expireMap[key] = time.Now().Add(expiration)
	} else {
		delete(c.expireMap, key)
	}

	return true
}

func (c *lfucache) UsedBytes() int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.usedBytes
}

func (c *lfucache) MaxBytes() int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.maxBytes
}

// SetMaxBytes will trigger eviction
func (c *lfucache) SetMaxBytes(maxBytes int64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.maxBytes = maxBytes
	if maxBytes > 0 {
		c.evict()
	}
}
