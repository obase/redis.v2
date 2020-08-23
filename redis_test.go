package redis

import (
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"
)

func TestPool(t *testing.T) {
	var demo = Get("demo")
	fmt.Println(Bool(demo.Do("EXISTS", "key1")))
	fmt.Println(Bool(demo.Do("SET", "key1", "123")))
	fmt.Println(String(demo.Do("GET", "key1")))
	fmt.Println(Int(demo.Do("GET", "key1")))
	fmt.Println(Int64(demo.Do("GET", "key1")))
	fmt.Println(Uint64(demo.Do("GET", "key1")))
	fmt.Println(Float64(demo.Do("GET", "key1")))
	fmt.Println(Bytes(demo.Do("GET", "key1")))

	replies, err := demo.Tx(func(b Bulk) {
		b.Do("HSET", "123", "name2", "jason")
		b.Do("HSET", "123", "age", 38)
		b.Do("HGETALL", "123")
		b.Do("HGET", "123", "name")
	})
	if err != nil {
		//panic(err)
	}
	fmt.Println(replies[0].String())
}

func TestGet(t *testing.T) {
	var demo = Get("demo")

	paras := 10000
	times := 100

	start := time.Now().UnixNano()
	wg := new(sync.WaitGroup)
	for i := 0; i < paras; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < times; j++ {
				demo.Do("GET", "key"+strconv.Itoa(j))
			}
		}()
	}
	wg.Wait()
	end := time.Now().UnixNano()
	fmt.Println("used:", end - start)
}
