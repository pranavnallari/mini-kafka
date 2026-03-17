package main

import (
	"fmt"

	"github.com/prananallari/mini-kafka/internal/partition"
)

func main() {
	keys := []string{"order-123", "order-456", "order-789"}
	partitionCount := uint32(3)
	for _, key := range keys {
		hash := partition.GetHash(key)
		fmt.Printf("key=%s partition=%d\n", key, hash%partitionCount)
	}
}
