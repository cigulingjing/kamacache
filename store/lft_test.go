package store

import (
	"fmt"
	"testing"
	"time"
)

type lftvalue struct {
	str string
	len int
}

func (v *lftvalue) Len() int {
	return v.len
}

func TestLFUCache(t *testing.T) {
	// 创建一个最大容量为 1000 字节的 LFU 缓存
	opts := Options{
		MaxBytes:        1000,
		OnEvicted:       nil,
		CleanupInterval: time.Minute,
	}
	cache := newLFUCache(opts)

	t.Run("Test set and get", func(t *testing.T) {
		// 测试 Set 和 Get 方法
		cache.Set("key1", &lftvalue{"value1", 6})
		value, ok := cache.Get("key1")
		if !ok || value.(*lftvalue).str != "value1" {
			t.Errorf("Expected value1, got %v", value)
		}
	})

	t.Run("Test set with expiration and GetWithExpiration", func(t *testing.T) {
		cache.SetWithExpiration("key2", &lftvalue{"value2", 6}, 1*time.Second)
		value, ttl, ok := cache.GetWithExpiration("key2")
		if !ok || value.(*lftvalue).str != "value2" || ttl <= 0 {
			t.Errorf("Expected value2 and valid TTL, got %v, %v, %v", value, ttl, ok)
		}
		time.Sleep(2 * time.Second)
		value, ttl, ok = cache.GetWithExpiration("key2")
		if ok {
			t.Errorf("Expected key2 to be expired, but it's not")
		}
	})

	t.Run("Test delete and clear", func(t *testing.T) {
		// 测试 Delete 方法
		cache.Delete("key1")
		_, ok := cache.Get("key1")
		if ok {
			t.Errorf("Expected key1 to be deleted, but it's still in the cache")
		}

		// 测试 Clear 方法
		cache.Set("key3", &lftvalue{"value3", 6})
		cache.Clear()
		_, ok = cache.Get("key3")
		if ok {
			t.Errorf("Expected cache to be cleared, but key3 is still in the cache")
		}
	})

	t.Run("Test UsedBytes and MaxBytes", func(t *testing.T) {
		// 测试 Len 方法
		cache.Set("key4", &lftvalue{"value4", 6})
		if cache.Len() != 1 {
			t.Errorf("Expected cache length to be 1, got %d", cache.Len())
		}
		// 测试 UsedBytes 和 MaxBytes 方法
		if cache.UsedBytes() != int64(len("key4")+6) {
			t.Errorf("Expected used bytes to be %d, got %d", int64(len("key4")+6), cache.UsedBytes())
		}
		if cache.MaxBytes() != 1000 {
			t.Errorf("Expected max bytes to be 1000, got %d", cache.MaxBytes())
		}

		// 测试 SetMaxBytes 方法
		cache.SetMaxBytes(500)
		if cache.MaxBytes() != 500 {
			t.Errorf("Expected max bytes to be 500, got %d", cache.MaxBytes())
		}
		cache.Clear()
	})

	t.Run("Test set and get expiration", func(t *testing.T) {
		// 测试 UpdateExpiration 方法
		cache.SetWithExpiration("key5", &lftvalue{"value5", 6}, 1*time.Second)
		cache.UpdateExpiration("key5", 2*time.Second)
		value, ttl, ok := cache.GetWithExpiration("key5")
		if !ok || value.(*lftvalue).str != "value5" || ttl <= 0 {
			t.Errorf("Expected value5 and valid TTL, got %v, %v, %v", value, ttl, ok)
		}

		// 测试 GetExpiration 方法
		expTime, ok := cache.GetExpiration("key5")
		if !ok || expTime.IsZero() {
			t.Errorf("Expected valid expiration time for key5, got %v, %v", expTime, ok)
		}
	})

	t.Run("Test LFU eviction", func(t *testing.T) {
		// 测试 LFU 替换策略
		cache.SetMaxBytes(100)
		for i := 0; i < 10; i++ {
			key := fmt.Sprintf("key%d", i)
			cache.Set(key, &lftvalue{fmt.Sprintf("value%d", i), 6})
		}
		for i := 0; i < 5; i++ {
			key := fmt.Sprintf("key%d", i)
			cache.Get(key)
		}
		// 此时应该优先删除没有访问过的键（频率为 1）
		cache.Set("newKey", &lftvalue{"newValue", 6})
		_, ok := cache.Get("key5")
		if ok {
			t.Errorf("Expected key5 to be evicted, but it's still in the cache")
		}
	})
}

type value struct {
	str string
	len int
}

func (v *value) Len() int {
	return v.len
}
