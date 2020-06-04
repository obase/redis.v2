package redis

import (
	"errors"
	"github.com/obase/conf"
	"time"
)

const CKEY = "redis"

type Config struct {
	Key string `json:"key" yaml:"key"`
	// Conn参数
	Network        string        `json:"network" yaml:"network"`               // 网络类簇,默认TCP
	Address        []string      `json:"address" yaml:"address"`               //连接的ip:port, 默认127.0.0.1:6379.
	Keepalive      time.Duration `json:"keepalive" yaml:"keepalive"`           //KeepAlive的间隔, 默认0不开启keepalive
	ConnectTimeout time.Duration `json:"connectTimeout" yaml:"connectTimeout"` //连接超时, 默认0不设置
	ReadTimeout    time.Duration `json:"readTimeout" yaml:"readTimeout"`       // 读超时, 默认0永远不超时
	WriteTimeout   time.Duration `json:"writeTimeout" yaml:"writeTimeout"`     // 写超时, 默认0永远不超时
	Password       string        `json:"password" yaml:"password"`             //密码
	// Pool参数
	InitConns       int           `json:"initConns" yaml:"initConns"`             //初始链接数, 默认0
	MaxConns        int           `json:"maxConns" yaml:"maxConns"`               //最大链接数, 默认0永远不限制
	MaxIdles        int           `json:"maxIdles" yaml:"maxIdles"`               //最大空闲数, 超出会在用完后自动关闭, 默认为InitConns
	TestIdleTimeout time.Duration `json:"testIdleTimeout" yaml:"testIdleTimeout"` //最大空闲超时, 超出会在获取时执行PING,如果失败则舍弃重建. 默认0表示不处理. 该选项是TestOnBorrow的一种优化
	ErrExceMaxConns bool          `json:"errExceMaxConns" yaml:"errExceMaxConns"` // 达到最大链接数, 是等待还是报错. 默认false等待 	// Key的统一后缀. 兼容此前的name情况, 不建议使用
	Select          int           `json:"select" yaml:"select"`                   // 选择DB下标, 默认0
	Cluster         bool          `json:"cluster" yaml:"cluster"`                 //是否集群
	// cluster参数
	Proxyips map[string]string `json:"proxyips" yaml:"proxyips"` //代理IP集合,一般用于本地测试用
}

func init() {
	configs, ok := conf.GetSlice(CKEY)
	if !ok {
		return
	}

	for _, config := range configs {
		if key, ok := conf.ElemString(config, "key"); ok {
			address, ok := conf.ElemStringSlice(config, "address")
			cluster, ok := conf.ElemBool(config, "cluster")
			password, ok := conf.ElemString(config, "password")
			keepalive, ok := conf.ElemDuration(config, "keepalive")
			connectTimeout, ok := conf.ElemDuration(config, "connectTimeout")
			if !ok {
				connectTimeout = 30 * time.Second
			}
			readTimeout, ok := conf.ElemDuration(config, "readTimeout")
			if !ok {
				readTimeout = 30 * time.Second
			}
			writeTimeout, ok := conf.ElemDuration(config, "writeTimeout")
			if !ok {
				writeTimeout = 30 * time.Second
			}
			initConns, ok := conf.ElemInt(config, "initConns")
			maxConns, ok := conf.ElemInt(config, "maxConns")
			if !ok {
				maxConns = 16
			}
			maxIdles, ok := conf.ElemInt(config, "maxIdles")
			if !ok {
				maxIdles = 16
			}
			testIdleTimeout, ok := conf.ElemDuration(config, "testIdleTimeout")
			errExceMaxConns, ok := conf.ElemBool(config, "errExceMaxConns")
			if !ok {
				errExceMaxConns = false
			}
			_select, ok := conf.ElemInt(config, "select")
			proxyips, ok := conf.ElemStringMap(config, "proxyips")

			option := &Config{
				Key:             key,
				Network:         "tcp",
				Address:         address,
				Keepalive:       keepalive,
				ConnectTimeout:  connectTimeout,
				ReadTimeout:     readTimeout,
				WriteTimeout:    writeTimeout,
				Password:        password,
				InitConns:       initConns,
				MaxConns:        maxConns,
				MaxIdles:        maxIdles,
				TestIdleTimeout: testIdleTimeout,
				ErrExceMaxConns: errExceMaxConns,
				Select:          _select,
				Cluster:         cluster,
				Proxyips:        proxyips,
			}

			if err := Setup(option); err != nil {
				panic(err)
			}
		}
	}
}

var instances = make(map[string]Redis)

func Setup(c *Config) (err error) {

	var keys = conf.ToStringSlice(c.Key)
	for _, key := range keys {
		if _, ok := instances[key]; ok {
			return errors.New("duplicate redis: " + key)
		}
	}

	var inst Redis
	if c.Cluster {
		inst, err = newRedisCluster(c)
	} else {
		inst, err = newRedisPool(c, "")
	}
	if err != nil {
		return
	}
	for _, key := range keys {
		instances[key] = inst
	}
	return
}

func Get(key string) (ret Redis) {
	ret = instances[key]
	return
}

func Must(key string) (ret Redis) {
	ret = instances[key]
	if ret == nil {
		panic("invalid redis: " + key)
	}
	return
}

type Bulk interface {
	Do(cmd string, keyArgs ...interface{})
}
type BulkCall func(b Bulk)
type BulkResp func(idx int, reply interface{}, err error) (interface{}, error)
type SubDataCall func(channel string, patter string, data []byte)
type SubStatCall func(channel string, kind string, count int)

type Redis interface {
	Do(cmd string, keyArgs ...interface{}) (reply interface{}, err error)
	Pi(call BulkCall, resp ...BulkResp) (err error)
	Tx(call BulkCall, resp ...BulkResp) (err error)
	Pub(key string, msg interface{}) (err error)
	Sub(key string, data SubDataCall, stat SubStatCall) (err error)
	Eval(script string, keys int, keysArgs ...interface{}) (reply interface{}, err error)
	Close()
}
