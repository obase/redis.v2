package redis

import (
	"fmt"
	"testing"
)

func TestBtree(t *testing.T) {
	var slots []*SlotInfo
	for i := 0; i < 16384; i += 3000 {
		slots = append(slots, &SlotInfo{
			Start: uint16(i + 1),
			End:   uint16(i + 3000),
		})
	}
	fmt.Println(slots)
	btree := BuildClusterBtree(slots)

	fmt.Println(btree)
}

func TestSlot(t *testing.T) {
	fmt.Println("this is a test")
}