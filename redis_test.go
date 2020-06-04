package redis

import (
	"fmt"
	"testing"
)

func TestPool(t *testing.T) {
	var demo = Get("demo")
	//fmt.Println(Bool(demo.Do("EXISTS", "key1")))
	//fmt.Println(Bool(demo.Do("SET", "key1", "123")))
	//fmt.Println(String(demo.Do("GET", "key1")))
	//fmt.Println(Int(demo.Do("GET", "key1")))
	//fmt.Println(Int64(demo.Do("GET", "key1")))
	//fmt.Println(Uint64(demo.Do("GET", "key1")))
	//fmt.Println(Float64(demo.Do("GET", "key1")))
	//fmt.Println(Bytes(demo.Do("GET", "key1")))

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
