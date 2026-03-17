package partition

import "hash/fnv"

func GetHash(key string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(key))
	hash := h.Sum32()
	return hash
}
