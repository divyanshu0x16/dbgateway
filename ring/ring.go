package ring

import (
	"hash/fnv"
	"sort"
	"strconv"
)

type Ring struct {
	hashes []uint32
	owners map[uint32]string
}

func New(nodes []string, replicas int) *Ring {
	r := &Ring{owners: make(map[uint32]string)}
	for _, node := range nodes {
		for i := 0; i < replicas; i++ {
			h := hash(node + "#" + strconv.Itoa(i))
			r.hashes = append(r.hashes, h)
			r.owners[h] = node
		}
	}
	sort.Slice(r.hashes, func(a, b int) bool { return r.hashes[a] < r.hashes[b] })
	return r
}

func (r *Ring) Get(key string) string {
	h := hash(key)
	i := sort.Search(len(r.hashes), func(i int) bool { return r.hashes[i] >= h })
	if i == len(r.hashes) {
		i = 0
	}
	return r.owners[r.hashes[i]]
}

func hash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}