package consistenthash

import (
	"errors"
	"fmt"
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// ConsHash implements a consistent hashing algorithm
type ConsHash struct {
	mu            sync.RWMutex
	config        *Config
	hashRing      []int
	hashToNodeMap map[int]string
	vritualNum    map[string]int   // Store total number of virtual nodes corresponding to record nodes
	nodeRequest   map[string]int64 // Store total request count for nodes
	totalRequests int64
}

func New(opts ...Option) *ConsHash {
	m := &ConsHash{
		config:        DefaultConfig,
		hashToNodeMap: make(map[int]string),
		vritualNum:    make(map[string]int),
		nodeRequest:   make(map[string]int64),
	}

	for _, opt := range opts {
		opt(m)
	}

	m.startBalancer() // Start load balancer
	return m
}

type Option func(*ConsHash)

func WithConfig(config *Config) Option {
	return func(m *ConsHash) {
		m.config = config
	}
}

// AddNode nodes to the hash ring
func (m *ConsHash) AddNode(nodes ...string) error {
	if len(nodes) == 0 {
		return errors.New("no nodes provided")
	}

	for _, node := range nodes {
		if node == "" {
			continue
		}
		m.mu.Lock()
		m.addVritualNode(node, m.config.DefaultReplicas)
		m.mu.Unlock()
	}

	return nil
}

// RemoveNode node from the hash ring
func (m *ConsHash) RemoveNode(node string) error {
	if node == "" {
		return errors.New("invalid node")
	}

	// Check if the node exists
	m.mu.RLock()
	replicas := m.vritualNum[node]
	m.mu.RUnlock()
	if replicas == 0 {
		return fmt.Errorf("node %s not found", node)
	}

	// Remove virtual nodes
	m.mu.Lock()
	m.removeWithVirtualNode(node, replicas)
	m.mu.Unlock()
	return nil
}

// Remove node and its virtual nodes
// ! Call this method must hold write lock
func (m *ConsHash) removeWithVirtualNode(node string, replicas int) {
	for i := 0; i < replicas; i++ {
		hash := int(m.config.HashFunc([]byte(fmt.Sprintf("%s-%d", node, i))))
		delete(m.hashToNodeMap, hash)
		for j := 0; j < len(m.hashRing); j++ {
			if m.hashRing[j] == hash {
				m.hashRing = append(m.hashRing[:j], m.hashRing[j+1:]...)
				break
			}
		}
	}
	delete(m.vritualNum, node)
	delete(m.nodeRequest, node)
}

// GetNode get the node which store the key
func (m *ConsHash) GetNode(key string) string {
	if key == "" {
		return ""
	}
	// Get read lock
	m.mu.RLock()

	if len(m.hashRing) == 0 {
		return ""
	}
	hash := int(m.config.HashFunc([]byte(key)))
	// Binary search
	idx := sort.Search(len(m.hashRing), func(i int) bool {
		return m.hashRing[i] >= hash
	})
	if idx == len(m.hashRing) {
		idx = 0
	}

	node := m.hashToNodeMap[m.hashRing[idx]]
	// Release read lock
	m.mu.RUnlock()

	m.mu.Lock()
	m.nodeRequest[node]++
	atomic.AddInt64(&m.totalRequests, 1)
	m.mu.Unlock()

	return node
}

// addVritualNode virtual nodes.
// ! Call this method must hold write lock
func (m *ConsHash) addVritualNode(node string, replicas int) {

	for i := 0; i < replicas; i++ {
		hash := int(m.config.HashFunc([]byte(fmt.Sprintf("%s-%d", node, i))))
		m.hashRing = append(m.hashRing, hash)
		m.hashToNodeMap[hash] = node
	}
	m.vritualNum[node] = replicas

	// When add new node, sort the hash ring
	sort.Ints(m.hashRing)
}

// checkAndRebalance check and rebalance load
func (m *ConsHash) checkAndRebalance() {

	m.mu.Lock()
	defer m.mu.Unlock()

	// Total number insufficient
	if atomic.LoadInt64(&m.totalRequests) < 1000 {
		return
	}

	// Use max differ measures load
	avgLoad := float64(m.totalRequests) / float64(len(m.vritualNum))
	var maxDiff float64
	for _, count := range m.nodeRequest {
		diff := math.Abs(float64(count) - avgLoad)
		if diff/avgLoad > maxDiff {
			maxDiff = diff / avgLoad
		}
	}

	// Differ beyond threshold
	if maxDiff > m.config.LoadBalanceThreshold {
		m.rebalanceNodes()
	}
}

// rebalanceNodes
func (m *ConsHash) rebalanceNodes() {

	avgLoad := float64(m.totalRequests) / float64(len(m.vritualNum))
	// Adjusting the number of virtual nodes
	for node, count := range m.nodeRequest {
		currentReplicas := m.vritualNum[node]
		loadRatio := float64(count) / avgLoad

		var newReplicas int
		if loadRatio > 1 {
			// Reduce virtual nodes
			newReplicas = int(float64(currentReplicas) / loadRatio)
		} else {
			// Add virtual nodes
			newReplicas = int(float64(currentReplicas) * (2 - loadRatio))
		}

		// Ensure newReplicas is within the limits
		if newReplicas < m.config.MinReplicas {
			newReplicas = m.config.MinReplicas
		}
		if newReplicas > m.config.MaxReplicas {
			newReplicas = m.config.MaxReplicas
		}

		if newReplicas != currentReplicas {
			// Remove node and add virtual nodes
			m.removeWithVirtualNode(node, m.vritualNum[node])
			m.addVritualNode(node, newReplicas)
		}
	}

	for node := range m.nodeRequest {
		m.nodeRequest[node] = 0
	}
	atomic.StoreInt64(&m.totalRequests, 0)

}

// GetStats get each node's request/total request ratio
func (m *ConsHash) GetStats() map[string]float64 {
	m.mu.RLock()
	defer m.mu.RUnlock()

	stats := make(map[string]float64)
	total := atomic.LoadInt64(&m.totalRequests)
	if total == 0 {
		return stats
	}

	for node, count := range m.nodeRequest {
		stats[node] = float64(count) / float64(total)
	}
	return stats
}

// startBalancer starts routine to run @checkAndRebalance
func (m *ConsHash) startBalancer() {
	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()

		for range ticker.C {
			m.checkAndRebalance()
		}
	}()
}
