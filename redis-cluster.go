package redis

import (
	"errors"
	"fmt"
	"github.com/gomodule/redigo/redis"
	"net"
	"sort"
	"strings"
	"sync"
)

var (
	ErrInvalidClusterSlots = errors.New("invalid cluster slots")
	ErrArgumentException   = errors.New("argument exception")
)

type redisCluster struct {
	*Config
	*sync.RWMutex
	Slots []*SlotInfo
	Btree *TreeNode
}

func newRedisCluster(c *Config) (ret *redisCluster, err error) {
	ret = &redisCluster{
		Config:  c,
		RWMutex: new(sync.RWMutex),
	}
	err = ret.UpdateClusterIndexes()
	if err != nil {
		return nil, err
	}
	return ret, nil
	return
}

// 指定写锁. 中间可能会更新cluster indexes
func (rc *redisCluster) UpdateClusterIndexes() (err error) {

	// 获取最新的slots并比较是否发生变化
	slots, err := QueryClusterSlots(rc.Config)
	if err != nil {
		return
	}
	btree := BuildClusterBtree(slots)

	rc.RWMutex.Lock()
	// 清除旧的连接
	rc.Close()
	rc.Slots = slots
	rc.Btree = btree
	// 初始新的连接
	for _, s := range slots {
		s.Pool, err = newRedisPool(rc.Config, s.Address)
		if err != nil {
			// 如果发生错误,需要级联清除已经创建的其他连接池
			rc.Close()
			break
		}
	}
	rc.RWMutex.Unlock()
	return
}

// 指定读锁. 中间可能会更新cluster indexes
func (rc *redisCluster) index(key string) (ret *pool) {
	v := Slot(key)
	rc.RWMutex.RLock()
	for n := rc.Btree; n != nil; {
		if v < n.Start {
			n = n.LNode
		} else if v > n.End {
			n = n.RNode
		} else {
			ret = n.Pool
			break
		}
	}
	rc.RWMutex.RUnlock()
	return ret
}

/*--------------------------接口方法----------------------------------*/
func (rc *redisCluster) Do(cmd string, keysArgs ...interface{}) (reply interface{}, err error) {
	reply, err = rc.index(keysArgs[0].(string)).Do(cmd, keysArgs...)
	if err != nil && IsSlotsError(err) {
		rc.UpdateClusterIndexes()
		reply, err = rc.index(keysArgs[0].(string)).Do(cmd, keysArgs...)
	}
	return

}

// 管道批量, 有可能部分成功.
func (rc *redisCluster) Pi(bf BulkCall, resps ...BulkResp) (err error) {
	err = rc.index("").Pi(bf, resps...)
	if err != nil && IsSlotsError(err) {
		rc.UpdateClusterIndexes()
		err = rc.index("").Pi(bf, resps...)
	}
	return
}

// 事务批量, 要么全部成功, 要么全部失败.
func (rc *redisCluster) Tx(bf BulkCall, resps ...BulkResp) (err error) {
	err = rc.index("").Tx(bf, resps...)
	if err != nil && IsSlotsError(err) {
		rc.UpdateClusterIndexes()
		err = rc.index("").Tx(bf, resps...)
	}
	return
}

// Publish
func (rc *redisCluster) Pub(key string, msg interface{}) (err error) {
	err = rc.index(key).Pub(key, msg)
	if err != nil && IsSlotsError(err) {
		rc.UpdateClusterIndexes()
		err = rc.index(key).Pub(key, msg)
	}
	return
}

// Subscribe, 阻塞执行sf直到返回stop或error才会结束
func (rc *redisCluster) Sub(key string, data SubDataCall, meta SubStatCall) (err error) {
	err = rc.index(key).Sub(key, data, meta)
	if err != nil && IsSlotsError(err) {
		rc.UpdateClusterIndexes()
		err = rc.index(key).Sub(key, data, meta)
	}
	return

}

func (rc *redisCluster) Eval(script string, keyCount int, keysArgs ...interface{}) (reply interface{}, err error) {
	if len(keysArgs) == 0 {
		return nil, ErrArgumentException
	}

	reply, err = rc.index(keysArgs[0].(string)).Eval(script, keyCount, keysArgs)
	if err != nil && IsSlotsError(err) {
		rc.UpdateClusterIndexes()
		reply, err = rc.index(keysArgs[0].(string)).Eval(script, keyCount, keysArgs)
	}
	return
}

func (rc *redisCluster) Close() {
	// 关闭操作不用锁,避免等待影响
	for _, s := range rc.Slots {
		if s != nil && s.Pool != nil {
			s.Pool.Close()
		}
	}
	rc.Slots = nil // 清空链
	rc.Btree = nil // 清链
}

/*--------------------------------集群辅助结构及方法----------------------------*/

type SlotInfo struct {
	Start   uint16
	End     uint16
	Address string
	Pool    *pool
}

type TreeNode struct {
	S *SlotInfo
	L *TreeNode
	R *TreeNode
}

func BuildClusterBtree(slots []*SlotInfo) *TreeNode {
	// 排序
	sort.Slice(slots, func(i, j int) bool {
		return slots[i].Start < slots[j].Start
	})
	// 建树
	return buildSortedSlots(slots)
}

func buildSortedSlots(slots []*SlotInfo) (btree *TreeNode) {
	switch l := len(slots); l {
	case 0:
	case 1:
		btree = &TreeNode{
			S: slots[0],
		}
	case 2:
		btree = &TreeNode{
			S: slots[1],
			L: &TreeNode{
				S: slots[0],
			},
		}
	case 3:
		btree = &TreeNode{
			S: slots[1],
			L: &TreeNode{
				S: slots[0],
			},
			R: &TreeNode{
				S: slots[2],
			},
		}
	default:
		m := l / 2
		btree = &TreeNode{
			S: slots[m],
			L: buildSortedSlots(slots[:m]),
			R: buildSortedSlots(slots[m+1:]),
		}
	}
	return
}

func QueryClusterSlots(opt *Config) ([]*SlotInfo, error) {

	var rc redis.Conn
	var err error
	var tcp net.Conn
	for _, address := range opt.Address {
		if tcp, err = net.DialTimeout(opt.Network, address, opt.ConnectTimeout); err == nil {
			rc = redis.NewConn(tcp, opt.ReadTimeout, opt.WriteTimeout)
			break
		}
	}
	if err != nil {
		return nil, err
	}
	defer rc.Close()

	if opt.Password != "" {
		if _, err = rc.Do("AUTH", opt.Password); err != nil {
			return nil, err
		}
	}

	infos, err := redis.Values(rc.Do("CLUSTER", "SLOTS"))
	if err != nil {
		return nil, err
	}

	/*-------------------------------------------------------------
	172.31.0.63:7000> CLUSTER SLOTS
	1) 1) (integer) 0
	   2) (integer) 5460
	   3) 1) "172.31.0.63"
		  2) (integer) 7000
		  3) "a585e144d73c9ca1c72e0dc14ba13b18eddddf61"
	   4) 1) "172.31.0.63"
		  2) (integer) 7003
		  3) "c079a3b1385faf1d1447b38d43941f75f2411f2b"
	2) 1) (integer) 5461
	   2) (integer) 10922
	   3) 1) "172.31.0.63"
		  2) (integer) 7001
		  3) "f0e5fd569ce7eaa63ab71174b7d4ae3cb34452b9"
	   4) 1) "172.31.0.63"
		  2) (integer) 7004
		  3) "b46b3907b9cbde9060715260684b64fe3fdd7729"
	3) 1) (integer) 10923
	   2) (integer) 16383
	   3) 1) "172.31.0.63"
		  2) (integer) 7002
		  3) "abfe332e0468b67cc63e4693d94273c4a135b448"
	   4) 1) "172.31.0.63"
		  2) (integer) 7005
		  3) "afb69dec265247d7d51496a7302922b0823a08fb"
	 --------------------------------------------------------------*/
	ret := make([]*SlotInfo, len(infos))
	plen := len(opt.Proxyips)
	for i, info := range infos {
		data := info.([]interface{})
		start, _, _ := Int(data[0], nil)
		end, _, _ := Int(data[1], nil)
		addrs := data[2].([]interface{})
		host, _, _ := String(addrs[0], nil)
		port, _, _ := Int(addrs[1], nil)

		// 替换成代理IP
		if plen > 0 {
			vhost := opt.Proxyips[host]
			if vhost != "" {
				host = vhost
			}
		}
		ret[i] = &SlotInfo{
			Start:   start,
			End:     end,
			Address: fmt.Sprintf("%s:%d", host, port),
		}
	}

	return ret, nil
}

func IsSlotsError(err error) bool {
	if rerr, ok := err.(redis.Error); ok {
		msg := rerr.Error()
		if strings.HasPrefix(msg, "MOVED") || strings.HasPrefix(msg, "ASK") {
			return true
		}
	} else if _, ok := err.(*net.OpError); ok {
		return true
	}
	return false
}

// 比较新旧slots是否发生变化,避免全局更新索引影响全面
func IsSlotsChanged(slot1, slot2 []*SlotInfo) bool {
	slen := 0
	if slen = len(slot1); slen != len(slot2) {
		return true
	}

	flags := make([]bool, slen)
	for _, s1 := range slot1 {
		found := false
		for j, s2 := range slot2 {
			if !flags[j] && s1.Start == s2.Start {
				flags[j], found = true, true
				if s1.End != s2.End || s1.Address != s2.Address {
					return true
				}
			}
		}
		if !found {
			return true
		}
	}
	return false
}

/*====================slots约定====================*/
const CLUSTER_SLOTS_NUMBER = 16384 //redis cluster fixed slots

var tab = [256]uint16{
	0x0000, 0x1021, 0x2042, 0x3063, 0x4084, 0x50a5, 0x60c6, 0x70e7,
	0x8108, 0x9129, 0xa14a, 0xb16b, 0xc18c, 0xd1ad, 0xe1ce, 0xf1ef,
	0x1231, 0x0210, 0x3273, 0x2252, 0x52b5, 0x4294, 0x72f7, 0x62d6,
	0x9339, 0x8318, 0xb37b, 0xa35a, 0xd3bd, 0xc39c, 0xf3ff, 0xe3de,
	0x2462, 0x3443, 0x0420, 0x1401, 0x64e6, 0x74c7, 0x44a4, 0x5485,
	0xa56a, 0xb54b, 0x8528, 0x9509, 0xe5ee, 0xf5cf, 0xc5ac, 0xd58d,
	0x3653, 0x2672, 0x1611, 0x0630, 0x76d7, 0x66f6, 0x5695, 0x46b4,
	0xb75b, 0xa77a, 0x9719, 0x8738, 0xf7df, 0xe7fe, 0xd79d, 0xc7bc,
	0x48c4, 0x58e5, 0x6886, 0x78a7, 0x0840, 0x1861, 0x2802, 0x3823,
	0xc9cc, 0xd9ed, 0xe98e, 0xf9af, 0x8948, 0x9969, 0xa90a, 0xb92b,
	0x5af5, 0x4ad4, 0x7ab7, 0x6a96, 0x1a71, 0x0a50, 0x3a33, 0x2a12,
	0xdbfd, 0xcbdc, 0xfbbf, 0xeb9e, 0x9b79, 0x8b58, 0xbb3b, 0xab1a,
	0x6ca6, 0x7c87, 0x4ce4, 0x5cc5, 0x2c22, 0x3c03, 0x0c60, 0x1c41,
	0xedae, 0xfd8f, 0xcdec, 0xddcd, 0xad2a, 0xbd0b, 0x8d68, 0x9d49,
	0x7e97, 0x6eb6, 0x5ed5, 0x4ef4, 0x3e13, 0x2e32, 0x1e51, 0x0e70,
	0xff9f, 0xefbe, 0xdfdd, 0xcffc, 0xbf1b, 0xaf3a, 0x9f59, 0x8f78,
	0x9188, 0x81a9, 0xb1ca, 0xa1eb, 0xd10c, 0xc12d, 0xf14e, 0xe16f,
	0x1080, 0x00a1, 0x30c2, 0x20e3, 0x5004, 0x4025, 0x7046, 0x6067,
	0x83b9, 0x9398, 0xa3fb, 0xb3da, 0xc33d, 0xd31c, 0xe37f, 0xf35e,
	0x02b1, 0x1290, 0x22f3, 0x32d2, 0x4235, 0x5214, 0x6277, 0x7256,
	0xb5ea, 0xa5cb, 0x95a8, 0x8589, 0xf56e, 0xe54f, 0xd52c, 0xc50d,
	0x34e2, 0x24c3, 0x14a0, 0x0481, 0x7466, 0x6447, 0x5424, 0x4405,
	0xa7db, 0xb7fa, 0x8799, 0x97b8, 0xe75f, 0xf77e, 0xc71d, 0xd73c,
	0x26d3, 0x36f2, 0x0691, 0x16b0, 0x6657, 0x7676, 0x4615, 0x5634,
	0xd94c, 0xc96d, 0xf90e, 0xe92f, 0x99c8, 0x89e9, 0xb98a, 0xa9ab,
	0x5844, 0x4865, 0x7806, 0x6827, 0x18c0, 0x08e1, 0x3882, 0x28a3,
	0xcb7d, 0xdb5c, 0xeb3f, 0xfb1e, 0x8bf9, 0x9bd8, 0xabbb, 0xbb9a,
	0x4a75, 0x5a54, 0x6a37, 0x7a16, 0x0af1, 0x1ad0, 0x2ab3, 0x3a92,
	0xfd2e, 0xed0f, 0xdd6c, 0xcd4d, 0xbdaa, 0xad8b, 0x9de8, 0x8dc9,
	0x7c26, 0x6c07, 0x5c64, 0x4c45, 0x3ca2, 0x2c83, 0x1ce0, 0x0cc1,
	0xef1f, 0xff3e, 0xcf5d, 0xdf7c, 0xaf9b, 0xbfba, 0x8fd9, 0x9ff8,
	0x6e17, 0x7e36, 0x4e55, 0x5e74, 0x2e93, 0x3eb2, 0x0ed1, 0x1ef0,
}

func Slot(key string) uint16 {
	bs := []byte(key)
	ln := len(bs)
	start, end := 0, ln
	for i := 0; i < ln; i++ {
		if bs[i] == '{' {
			for j := i + 1; j < ln; j++ {
				if bs[j] == '}' {
					start, end = i, j
					break
				}
			}
			break
		}
	}
	crc := uint16(0)
	for i := start; i < end; i++ {
		index := byte(crc>>8) ^ bs[i]
		crc = (crc << 8) ^ tab[index]
	}
	return crc % CLUSTER_SLOTS_NUMBER
}
