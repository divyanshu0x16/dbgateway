// Package hashring implements a consistent-hashing ring used to route keys
// to backend shards, mirroring how ZGateway maps keys to ZippyDB shards.
package hashring

import (
	"hash/fnv"
	"sort"
	"strconv"
	"sync"
)

const defaultVirtualNodes = 100

// Ring is a consistent-hashing ring mapping keys to shard names.
type Ring struct {
	mu           sync.RWMutex
	virtualNodes int
	sortedHashes []uint32
	hashToShard  map[uint32]string
}

// New builds a Ring seeded with the given shard names.
func New(shards []string) *Ring {
	r := &Ring{
		virtualNodes: defaultVirtualNodes,
		hashToShard:  make(map[uint32]string),
	}
	for _, s := range shards {
		r.Add(s)
	}
	return r
}

// Add inserts a shard (and its virtual nodes) into the ring.
func (r *Ring) Add(shard string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	for i := 0; i < r.virtualNodes; i++ {
		h := hashKey(shard + "#" + strconv.Itoa(i))
		r.hashToShard[h] = shard
	}
	r.rebuildSortedLocked()
}

// Remove takes a shard (and its virtual nodes) out of the ring.
func (r *Ring) Remove(shard string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	for i := 0; i < r.virtualNodes; i++ {
		h := hashKey(shard + "#" + strconv.Itoa(i))
		delete(r.hashToShard, h)
	}
	r.rebuildSortedLocked()
}

// Get returns the shard responsible for key.
func (r *Ring) Get(key string) (string, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.sortedHashes) == 0 {
		return "", false
	}

	h := hashKey(key)
	idx := sort.Search(len(r.sortedHashes), func(i int) bool {
		return r.sortedHashes[i] >= h
	})
	if idx == len(r.sortedHashes) {
		idx = 0
	}
	return r.hashToShard[r.sortedHashes[idx]], true
}

// Shards returns the distinct shard names currently in the ring.
func (r *Ring) Shards() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	seen := make(map[string]struct{})
	out := make([]string, 0)
	for _, s := range r.hashToShard {
		if _, ok := seen[s]; !ok {
			seen[s] = struct{}{}
			out = append(out, s)
		}
	}
	sort.Strings(out)
	return out
}

func (r *Ring) rebuildSortedLocked() {
	hashes := make([]uint32, 0, len(r.hashToShard))
	for h := range r.hashToShard {
		hashes = append(hashes, h)
	}
	sort.Slice(hashes, func(i, j int) bool { return hashes[i] < hashes[j] })
	r.sortedHashes = hashes
}

func hashKey(key string) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return h.Sum32()
}
