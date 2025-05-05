package store

import "errors"

var (
	ErrCacheNotFound   = errors.New("cache not found")    // 缓存未找到错误
	ErrMinFreqNotExist = errors.New("min freq not exist") // 最小频率不存在错误
)
