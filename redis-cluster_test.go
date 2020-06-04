package redis

import (
	"fmt"
	"math/rand"
	"testing"
)

var index []*SlotInfo
var slots []*SlotInfo
var btree *TreeNode

func init2() {
	index = make([]*SlotInfo, 16384+1)
	var start, end int
	for start < 16384 {

		end = start + 3000
		if end > 16384 {
			end = 16384
		}

		s := &SlotInfo{
			Start: uint16(start),
			End:   uint16(end),
		}
		slots = append(slots, s)
		for i := s.Start; i <= s.End; i++ {
			index[i] = s
		}
		start = end + 1
	}
	for _, s := range slots {
		fmt.Println(*s)
	}
	//btree = BuildClusterBtree(slots)
}

func BenchmarkIndex(b *testing.B) {
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		v := uint16(rand.Intn(16384))
		_ = index[v]
	}
	b.StopTimer()
}

func BenchmarkArray(b *testing.B) {
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		v := uint16(rand.Intn(16384))
		for _, s := range slots {
			if v>=s.Start && v <= s.End {
				_ = s.Pool
				break
			}
		}
	}
	b.StopTimer()
}

func BenchmarkBinary(b *testing.B) {
	n := uint16(len(slots))
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		v := uint16(rand.Intn(16384)) + 1
		l, h := uint16(0), n-1
	__LOOP:
		for l <= h {
			m := (l + h) / 2
			s := slots[m]

			switch {
			case v < s.Start:
				h = m - 1
			case v > s.End:
				l = m + 1
			default:
				_ = s.Pool
				break __LOOP
			}
		}
	}
	b.StopTimer()
}

func BenchmarkBtree(b *testing.B) {
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		v := uint16(rand.Intn(16384))
		for n := btree; n != nil; {
			if v < n.S.Start {
				n = n.L
			} else if v > n.S.End {
				n = n.R
			} else {
				_ = n.S.Pool
				break
			}
		}
	}
	b.StopTimer()
}
