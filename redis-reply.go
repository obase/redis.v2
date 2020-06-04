package redis

import (
	"fmt"
	"github.com/gomodule/redigo/redis"
	"strconv"
)

type ValueScore struct {
	Value string
	Score float64
}

func Bool(reply interface{}, err error) (bool, bool, error) {
	if err != nil {
		if err == redis.ErrNil {
			return false, false, nil
		}
		return false, false, err
	}
	switch reply := reply.(type) {
	case int64:
		return reply != 0, true, nil
	case []byte:
		ret, err := strconv.ParseBool(string(reply))
		return ret, true, err
	case string:
		if reply == "OK" {
			return true, true, err
		}
		ret, err := strconv.ParseBool(reply)
		return ret, true, err
	case nil:
		return false, false, nil
	case redis.Error:
		return false, false, reply
	}
	return false, false, fmt.Errorf("unexpected type for Bool %T", reply)
}

func Int(reply interface{}, err error) (int, bool, error) {
	if err != nil {
		if err == redis.ErrNil {
			return 0, false, nil
		}
		return 0, false, err
	}
	switch reply := reply.(type) {
	case int64:
		return int(reply), true, nil
	case []byte:
		n, err := strconv.ParseInt(string(reply), 10, 0)
		return int(n), true, err
	case string:
		n, err := strconv.ParseInt(reply, 10, 0)
		return int(n), true, err
	case nil:
		return 0, false, nil
	case redis.Error:
		return 0, false, reply
	}
	return 0, false, fmt.Errorf("unexpected type for Int %T", reply)
}

func Int64(reply interface{}, err error) (int64, bool, error) {
	if err != nil {
		if err == redis.ErrNil {
			return 0, false, nil
		}
		return 0, false, err
	}
	switch reply := reply.(type) {
	case int64:
		return reply, true, nil
	case []byte:
		n, err := strconv.ParseInt(string(reply), 10, 64)
		return n, true, err
	case string:
		n, err := strconv.ParseInt(reply, 10, 64)
		return n, true, err
	case nil:
		return 0, false, nil
	case redis.Error:
		return 0, false, reply
	}
	return 0, false, fmt.Errorf("unexpected type for Int64 %T", reply)
}

func Uint64(reply interface{}, err error) (uint64, bool, error) {
	if err != nil {
		if err == redis.ErrNil {
			return 0, false, nil
		}
		return 0, false, err
	}
	switch reply := reply.(type) {
	case int64:
		if reply < 0 {
			return uint64(reply), true, fmt.Errorf("unexpected negative value %v for Uint64", reply)
		}
		return uint64(reply), true, nil
	case []byte:
		n, err := strconv.ParseUint(string(reply), 10, 64)
		return n, true, err
	case string:
		n, err := strconv.ParseUint(reply, 10, 64)
		return n, true, err
	case nil:
		return 0, false, nil
	case redis.Error:
		return 0, false, reply
	}
	return 0, false, fmt.Errorf("unexpected type for Uint64 %T", reply)
}

func Float64(reply interface{}, err error) (float64, bool, error) {
	if err != nil {
		if err == redis.ErrNil {
			return 0, false, nil
		}
		return 0, false, err
	}
	switch reply := reply.(type) {
	case []byte:
		n, err := strconv.ParseFloat(string(reply), 64)
		return n, true, err
	case string:
		n, err := strconv.ParseFloat(reply, 64)
		return n, true, err
	case int64:
		return float64(reply), true, nil
	case nil:
		return 0, false, nil
	case redis.Error:
		return 0, false, reply
	}
	return 0, false, fmt.Errorf("unexpected type for Float64  %T", reply)
}

func String(reply interface{}, err error) (string, bool, error) {
	if err != nil {
		if err == redis.ErrNil {
			return "", false, nil
		}
		return "", false, err
	}
	switch reply := reply.(type) {
	case []byte:
		return string(reply), true, nil
	case string:
		return reply, true, nil
	case int64:
		return strconv.FormatInt(reply, 10), true, nil
	case nil:
		return "", false, nil
	case redis.Error:
		return "", false, reply
	}
	return "", false, fmt.Errorf("unexpected type for String %T", reply)
}

func Bytes(reply interface{}, err error) ([]byte, bool, error) {
	if err != nil {
		if err == redis.ErrNil {
			return nil, false, nil
		}
		return nil, false, err
	}
	switch reply := reply.(type) {
	case []byte:
		return reply, true, nil
	case string:
		return []byte(reply), true, nil
	case int64:
		return []byte(strconv.FormatInt(reply, 10)), true, nil
	case nil:
		return nil, false, nil
	case redis.Error:
		return nil, false, reply
	}
	return nil, false, fmt.Errorf("unexpected type for Bytes %T", reply)
}

func BoolSlice(reply interface{}, err error) ([]bool, bool, error) {
	if err != nil {
		if err == redis.ErrNil {
			return nil, false, nil
		}
		return nil, false, err
	}
	switch reply := reply.(type) {
	case []interface{}:
		ret := make([]bool, len(reply))
		for i, vi := range reply {
			v, _, err := Bool(vi, nil)
			if err != nil {
				return nil, true, err
			} else {
				ret[i] = v
			}
		}
		return ret, true, nil
	case nil:
		return nil, false, nil
	case redis.Error:
		return nil, false, reply
	}
	v, _, err := Bool(reply, nil)
	if err != nil {
		return nil, true, err
	} else {
		return []bool{v}, true, nil
	}
}

func IntSlice(reply interface{}, err error) ([]int, bool, error) {
	if err != nil {
		if err == redis.ErrNil {
			return nil, false, nil
		}
		return nil, false, err
	}
	switch reply := reply.(type) {
	case []interface{}:
		ret := make([]int, len(reply))
		for i, vi := range reply {
			v, _, err := Int(vi, nil)
			if err != nil {
				return nil, true, err
			} else {
				ret[i] = v
			}
		}
		return ret, true, nil
	case nil:
		return nil, false, nil
	case redis.Error:
		return nil, false, reply
	}
	v, _, err := Int(reply, nil)
	if err != nil {
		return nil, true, err
	} else {
		return []int{v}, true, nil
	}
}

func Int64Slice(reply interface{}, err error) ([]int64, bool, error) {
	if err != nil {
		if err == redis.ErrNil {
			return nil, false, nil
		}
		return nil, false, err
	}
	switch reply := reply.(type) {
	case []interface{}:
		ret := make([]int64, len(reply))
		for i, vi := range reply {
			v, _, err := Int64(vi, nil)
			if err != nil {
				return nil, true, err
			} else {
				ret[i] = v
			}
		}
		return ret, true, nil
	case nil:
		return nil, false, nil
	case redis.Error:
		return nil, false, reply
	}
	v, _, err := Int64(reply, nil)
	if err != nil {
		return nil, true, err
	} else {
		return []int64{v}, true, nil
	}
}

func Uint64Slice(reply interface{}, err error) ([]uint64, bool, error) {
	if err != nil {
		if err == redis.ErrNil {
			return nil, false, nil
		}
		return nil, false, err
	}
	switch reply := reply.(type) {
	case []interface{}:
		ret := make([]uint64, len(reply))
		for i, vi := range reply {
			v, _, err := Uint64(vi, nil)
			if err != nil {
				return nil, true, err
			} else {
				ret[i] = v
			}
		}
		return ret, true, nil
	case nil:
		return nil, false, nil
	case redis.Error:
		return nil, false, reply
	}
	v, _, err := Uint64(reply, nil)
	if err != nil {
		return nil, true, err
	} else {
		return []uint64{v}, true, nil
	}
}

func Float64Slice(reply interface{}, err error) ([]float64, bool, error) {
	if err != nil {
		if err == redis.ErrNil {
			return nil, false, nil
		}
		return nil, false, err
	}
	switch reply := reply.(type) {
	case []interface{}:
		ret := make([]float64, len(reply))
		for i, vi := range reply {
			v, _, err := Float64(vi, nil)
			if err != nil {
				return nil, true, err
			} else {
				ret[i] = v
			}
		}
		return ret, true, nil
	case nil:
		return nil, false, nil
	case redis.Error:
		return nil, false, reply
	}
	v, _, err := Float64(reply, nil)
	if err != nil {
		return nil, true, err
	} else {
		return []float64{v}, true, nil
	}
}

func StringSlice(reply interface{}, err error) ([]string, bool, error) {
	if err != nil {
		if err == redis.ErrNil {
			return nil, false, nil
		}
		return nil, false, err
	}
	switch reply := reply.(type) {
	case []interface{}:
		ret := make([]string, len(reply))
		for i, vi := range reply {
			v, _, err := String(vi, nil)
			if err != nil {
				return nil, true, err
			} else {
				ret[i] = v
			}
		}
		return ret, true, nil
	case nil:
		return nil, false, nil
	case redis.Error:
		return nil, false, reply
	}
	v, _, err := String(reply, nil)
	if err != nil {
		return nil, true, err
	} else {
		return []string{v}, true, nil
	}
}

func BytesSlice(reply interface{}, err error) ([][]byte, bool, error) {
	if err != nil {
		if err == redis.ErrNil {
			return nil, false, nil
		}
		return nil, false, err
	}
	switch reply := reply.(type) {
	case []interface{}:
		ret := make([][]byte, len(reply))
		for i, vi := range reply {
			v, _, err := Bytes(vi, nil)
			if err != nil {
				return nil, true, err
			} else {
				ret[i] = v
			}
		}
		return ret, true, nil
	case nil:
		return nil, false, nil
	case redis.Error:
		return nil, false, reply
	}
	v, _, err := Bytes(reply, nil)
	if err != nil {
		return nil, true, err
	} else {
		return [][]byte{v}, true, nil
	}
}

func ValueScoreSlice(reply interface{}, err error) ([]*ValueScore, bool, error) {
	if err != nil {
		if err == redis.ErrNil {
			return nil, false, nil
		}
		return nil, false, err
	}

	switch reply := reply.(type) {
	case []interface{}:
		len := len(reply)
		ret := make([]*ValueScore, len/2)
		for i, j := 1, 0; i < len; i += 2 {
			k, _, err := String(reply[i-1], nil)
			if err != nil {
				return nil, true, err
			}
			v, _, err := Float64(reply[i], nil)
			if err != nil {
				return nil, true, err
			}
			ret[j] = &ValueScore{
				Value: k,
				Score: v,
			}
			j++
		}
		return ret, true, nil
	case nil:
		return nil, false, nil
	case redis.Error:
		return nil, false, reply
	}
	return nil, true, fmt.Errorf("unexpected type for ValueScoreSlice %T", reply)
}

func Slice(reply interface{}, err error) ([]interface{}, bool, error) {
	if err != nil {
		return nil, false, err
	}
	switch reply := reply.(type) {
	case nil:
		return nil, false, redis.ErrNil
	case []interface{}:
		return reply, true, nil
	case redis.Error:
		return nil, false, reply
	}
	return nil, false, fmt.Errorf("unexpected type for Values %T", reply)
}

func Map(reply interface{}, err error) (map[string]interface{}, bool, error) {
	if err != nil {
		if err == redis.ErrNil {
			return nil, false, nil
		}
		return nil, false, err
	}

	switch reply := reply.(type) {
	case []interface{}:
		len := len(reply)
		ret := make(map[string]interface{}, len/2)
		for i := 1; i < len; i += 2 {
			k, _, err := String(reply[i-1], nil)
			if err != nil {
				return nil, true, err
			}
			ret[k] = reply[i]
		}
		return ret, true, nil
	case nil:
		return nil, false, nil
	case redis.Error:
		return nil, false, reply
	}
	return nil, true, fmt.Errorf("unexpected type for Map %T", reply)
}

func BoolMap(reply interface{}, err error) (map[string]bool, bool, error) {
	if err != nil {
		if err == redis.ErrNil {
			return nil, false, nil
		}
		return nil, false, err
	}

	switch reply := reply.(type) {
	case []interface{}:
		len := len(reply)
		ret := make(map[string]bool, len/2)
		for i := 1; i < len; i += 2 {
			k, _, err := String(reply[i-1], nil)
			if err != nil {
				return nil, true, err
			}
			v, _, err := Bool(reply[i], nil)
			if err != nil {
				return nil, true, err
			}
			ret[k] = v
		}
		return ret, true, nil
	case nil:
		return nil, false, nil
	case redis.Error:
		return nil, false, reply
	}
	return nil, true, fmt.Errorf("unexpected type for Map %T", reply)
}

func IntMap(reply interface{}, err error) (map[string]int, bool, error) {
	if err != nil {
		if err == redis.ErrNil {
			return nil, false, nil
		}
		return nil, false, err
	}

	switch reply := reply.(type) {
	case []interface{}:
		len := len(reply)
		ret := make(map[string]int, len/2)
		for i := 1; i < len; i += 2 {
			k, _, err := String(reply[i-1], nil)
			if err != nil {
				return nil, true, err
			}
			v, _, err := Int(reply[i], nil)
			if err != nil {
				return nil, true, err
			}
			ret[k] = v
		}
		return ret, true, nil
	case nil:
		return nil, false, nil
	case redis.Error:
		return nil, false, reply
	}
	return nil, true, fmt.Errorf("unexpected type for IntMap %T", reply)
}

func Int64Map(reply interface{}, err error) (map[string]int64, bool, error) {
	if err != nil {
		if err == redis.ErrNil {
			return nil, false, nil
		}
		return nil, false, err
	}

	switch reply := reply.(type) {
	case []interface{}:
		len := len(reply)
		ret := make(map[string]int64, len/2)
		for i := 1; i < len; i += 2 {
			k, _, err := String(reply[i-1], nil)
			if err != nil {
				return nil, true, err
			}
			v, _, err := Int64(reply[i], nil)
			if err != nil {
				return nil, true, err
			}
			ret[k] = v
		}
		return ret, true, nil
	case nil:
		return nil, false, nil
	case redis.Error:
		return nil, false, reply
	}
	return nil, true, fmt.Errorf("unexpected type for Int64Map %T", reply)
}

func Uint64Map(reply interface{}, err error) (map[string]uint64, bool, error) {
	if err != nil {
		if err == redis.ErrNil {
			return nil, false, nil
		}
		return nil, false, err
	}

	switch reply := reply.(type) {
	case []interface{}:
		len := len(reply)
		ret := make(map[string]uint64, len/2)
		for i := 1; i < len; i += 2 {
			k, _, err := String(reply[i-1], nil)
			if err != nil {
				return nil, true, err
			}
			v, _, err := Uint64(reply[i], nil)
			if err != nil {
				return nil, true, err
			}
			ret[k] = v
		}
		return ret, true, nil
	case nil:
		return nil, false, nil
	case redis.Error:
		return nil, false, reply
	}
	return nil, true, fmt.Errorf("unexpected type for Uint64Map %T", reply)
}

func Float64Map(reply interface{}, err error) (map[string]float64, bool, error) {
	if err != nil {
		if err == redis.ErrNil {
			return nil, false, nil
		}
		return nil, false, err
	}

	switch reply := reply.(type) {
	case []interface{}:
		len := len(reply)
		ret := make(map[string]float64, len/2)
		for i := 1; i < len; i += 2 {
			k, _, err := String(reply[i-1], nil)
			if err != nil {
				return nil, true, err
			}
			v, _, err := Float64(reply[i], nil)
			if err != nil {
				return nil, true, err
			}
			ret[k] = v
		}
		return ret, true, nil
	case nil:
		return nil, false, nil
	case redis.Error:
		return nil, false, reply
	}
	return nil, true, fmt.Errorf("unexpected type for Float64Map %T", reply)
}

func StringMap(reply interface{}, err error) (map[string]string, bool, error) {
	if err != nil {
		if err == redis.ErrNil {
			return nil, false, nil
		}
		return nil, false, err
	}

	switch reply := reply.(type) {
	case []interface{}:
		len := len(reply)
		ret := make(map[string]string, len/2)
		for i := 1; i < len; i += 2 {
			k, _, err := String(reply[i-1], nil)
			if err != nil {
				return nil, true, err
			}
			v, _, err := String(reply[i], nil)
			if err != nil {
				return nil, true, err
			}
			ret[k] = v
		}
		return ret, true, nil
	case nil:
		return nil, false, nil
	case redis.Error:
		return nil, false, reply
	}
	return nil, true, fmt.Errorf("unexpected type for StringMap %T", reply)
}

func BytesMap(reply interface{}, err error) (map[string][]byte, bool, error) {
	if err != nil {
		if err == redis.ErrNil {
			return nil, false, nil
		}
		return nil, false, err
	}

	switch reply := reply.(type) {
	case []interface{}:
		len := len(reply)
		ret := make(map[string][]byte, len/2)
		for i := 1; i < len; i += 2 {
			k, _, err := String(reply[i-1], nil)
			if err != nil {
				return nil, true, err
			}
			v, _, err := Bytes(reply[i], nil)
			if err != nil {
				return nil, true, err
			}
			ret[k] = v
		}
		return ret, true, nil
	case nil:
		return nil, false, nil
	case redis.Error:
		return nil, false, reply
	}
	return nil, true, fmt.Errorf("unexpected type for BytesMap %T", reply)
}
