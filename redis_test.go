package redis

import (
	"github.com/obase/redis"
	"testing"
)

var demo = Get("demo")
func TestConn_Do(t *testing.T) {
	bv, ok, err := redis.Bool(demo.Do("EXITST","key1"))
}
