package redis

import (
	"errors"
	"github.com/gomodule/redigo/redis"
	"net"
	"sync"
	"time"
)

var ErrExceedMaxConns = errors.New("redis pool exceed max connections")

type conn struct {
	P    *pool
	C    redis.Conn
	T    int64
	Prev *conn
	Next *conn

	rcv int // 配合bulk操作, 执行命令的数量
}

func (c *conn) Reset() {
	c.rcv = 0
}

func (c *conn) Do(cmd string, keyArgs ...interface{}) {
	c.rcv++
	c.C.Send(cmd, keyArgs...)
}

type pool struct {
	*Config
	*sync.Mutex
	*sync.Cond
	Address      string // 用于集群模式覆盖config中的address配置
	TestIdleSecs int64
	Nalls        int   // 所有数量
	Nfree        int   // 空闲数量
	Lfree        *conn // 空闲链表头
	Tfree        *conn // 空闲链表尾
	Lused        *conn // 在用链表头
}

func newRedisPool(c *Config, address string) (*pool, error) {
	mux := new(sync.Mutex)
	p := &pool{
		Config:       c,
		Mutex:        mux,
		Cond:         sync.NewCond(mux),
		Address:      nvl(address, c.Address[0]),
		TestIdleSecs: int64(c.TestIdleTimeout.Seconds()),
	}
	for i := 0; i < c.InitConns; i++ {
		_, err := p.Create(false)
		if err != nil {
			// 清理已经建好的链接
			p.Foreach(func(c *conn) {
				c.C.Close()
			})
			return nil, err
		}
	}
	return p, nil
}

func nvl(v1 string, v2 string) string {
	if v1 != "" {
		return v1
	}
	return v2
}

/*************************START: 连接操作*******************************/
func (p *pool) new() (ret *conn, err error) {
	s, err := net.DialTimeout(p.Config.Network, p.Address, p.Config.ConnectTimeout)
	if err != nil {
		return
	}
	tcp := s.(*net.TCPConn)
	if p.Config.Keepalive > 0 {
		tcp.SetKeepAlive(true)
		tcp.SetKeepAlivePeriod(p.Config.Keepalive)
	}
	c := redis.NewConn(tcp, p.Config.ReadTimeout, p.Config.WriteTimeout)
	if p.Config.Password != "" {
		_, err = c.Do("AUTH", p.Config.Password)
		if err != nil {
			return
		}
	}
	// 如果不是集群,则支持select
	if !p.Config.Cluster && p.Config.Select > 0 {
		_, err = c.Do("SELECT", p.Config.Select)
		if err != nil {
			return
		}
	}

	// 创建元素
	ret = &conn{
		P: p,
		C: c,
		T: time.Now().Unix(), // 链接以及其放入时间
	}
	return
}

/*************************START: 链表操作*******************************/
/*
链表基础操作有4种:
1. 借: 将idle头的元素指向used尾
2. 还: 将conn指向idle尾的位置
3. 加: 将conn指向idle尾的位置
4. 删: 将conn的前后对接起来,并将自尾置空
*/

func (p *pool) Foreach(f func(c *conn)) {
	for e := p.Lused; e != nil; e = e.Next {
		f(e)
	}
	for e := p.Lfree; e != nil; e = e.Next {
		f(e)
	}
}

func (p *pool) Create(used bool) (c *conn, err error) {
	c, err = p.new()
	if err != nil {
		return
	}
	if used {
		//生成直接使用. 在used头插入,默认conn.Nxt为nil
		if p.Lused != nil {
			p.Lused.Prev = c
			c.Next = p.Lused
		}
		p.Lused = c
	} else {
		if p.Lfree != nil {
			p.Lfree.Prev = c
			c.Next = p.Lfree
		}
		p.Lfree = c

		if p.Tfree == nil {
			p.Tfree = c
		}

		p.Nfree++
	}
	p.Nalls++

	return
}

func (p *pool) Remove(c *conn) {
	if n := c.Prev; n != nil {
		n.Next = c.Next
	}
	if n := c.Next; n != nil {
		n.Prev = c.Prev
	}
	p.Nalls--
}

func (p *pool) Borrow() (c *conn) {
	if p.Tfree != nil {
		c = p.Tfree
		p.Tfree = c.Prev
		if p.Tfree != nil {
			p.Tfree.Next = nil // 必须重置后继,否则影响Remove
		}
		if p.Lused != nil {
			p.Lused.Prev = c
			c.Next = p.Lused
		}
		p.Lused = c
		p.Lused.Prev = nil // 必须重置前驱,否则影响Remove

		p.Nfree--
	}
	return
}

func (p *pool) Return(c *conn) {
	c.T = time.Now().Unix()
	if n := c.Prev; n != nil {
		n.Next = c.Next
	}
	if n := c.Next; n != nil {
		n.Prev = c.Prev
	}

	if p.Lfree != nil {
		p.Lfree.Prev = c
		c.Next = p.Lfree
	}
	p.Lfree = c
	p.Lfree.Prev = nil
	if p.Tfree == nil {
		p.Tfree = c
	}
	p.Nfree++
}

/*************************START: 池化操作*******************************/
func (p *pool) Get() (ret *conn, err error) {
	for {
		p.Mutex.Lock()
		if p.Config.MaxConns > 0 {
			for p.Nfree == 0 && p.Nalls >= p.Config.MaxConns {
				if p.ErrExceMaxConns {
					p.Mutex.Unlock()
					return nil, ErrExceedMaxConns
				} else {
					p.Cond.Wait()
				}
			}
		}
		if p.Nfree > 0 {
			ret = p.Borrow()
		} else {
			ret, err = p.Create(true)
		}
		p.Mutex.Unlock()
		if err != nil {
			return
		}

		if p.TestIdleSecs == 0 || ret.T+p.TestIdleSecs > time.Now().Unix() {
			//  无需检测或未超时,直接返回
			return
		} else if _, err = ret.C.Do("PING"); err == nil {
			ret.T = time.Now().Unix()
			// 检测通过, 直接返回
			return
		}
		p.Put(ret) //回收销毁,再从循环获取下一个
	}
	return
}

func (p *pool) Put(c *conn) {
	if c == nil {
		return
	}
	broken := c.C.Err() != nil
	p.Mutex.Lock()
	if broken {
		p.Remove(c)
	} else {
		p.Return(c)
	}
	p.Cond.Signal()
	p.Mutex.Unlock()
	if broken {
		//销毁有问题的连接
		c.C.Close()
	}
}

/*************************START: 接口操作*******************************/
func (p *pool) Do(cmd string, keysArgs ...interface{}) (reply interface{}, err error) {
	rc, err := p.Get()
	if err == nil {
		reply, err = rc.C.Do(cmd, keysArgs...)
	}
	p.Put(rc)
	return
}

func (p *pool) Pi(call BulkCall) (ret []*BulkResp, err error) {
	rc, err := p.Get()
	if err == nil {
		rc.Reset() // 重置计数
		call(rc)
		if rc.rcv > 0 {
			err = rc.C.Flush()
			if err == nil {
				ret = make([]*BulkResp, rc.rcv)
				for i := 0; i < rc.rcv; i++ {
					r, e := rc.C.Receive()
					ret[i] = &BulkResp{
						Reply: r,
						Error: e,
					}
				}
			}
		}
	}
	p.Put(rc)
	return
}

func (p *pool) Ex(call BulkCall) (ret interface{}, err error) {
	rc, err := p.Get()
	if err == nil {
		rc.Reset() // 重置计数
		rc.C.Send("MULTI")
		call(rc)
		if rc.rcv > 0 {
			ret, err = rc.C.Do("EXEC")
		} else {
			rc.C.Do("DISCARD")
		}
	}
	p.Put(rc)
	return
}

func (p *pool) Tx(call BulkCall) (ret []*BulkResp, err error) {
	rc, err := p.Get()
	if err == nil {
		rc.Reset() // 重置计数
		rc.C.Send("MULTI")
		call(rc)
		if rc.rcv > 0 {
			var tmp []interface{}
			tmp, _, err = Slice(rc.C.Do("EXEC"))
			if err == nil {
				ret = make([]*BulkResp, len(tmp))
				for i, t := range tmp {
					ret[i] = &BulkResp{
						Reply: t,
						Error: err,
					}
				}
			}
		} else {
			rc.C.Do("DISCARD")
		}
	}
	p.Put(rc)
	return
}

func (p *pool) Pub(key string, msg interface{}) (err error) {
	rc, err := p.Get()
	if err == nil {
		_, err = rc.C.Do("PUBLISH", key, msg)
	}
	rc.P.Put(rc)
	return
}

func (p *pool) Sub(key string, data SubDataCall, stat SubStatCall) (err error) {

	rc, err := p.Get()
	if err == nil {
		psc := redis.PubSubConn{Conn: rc.C}
		psc.Subscribe(key)
		for {
			switch v := psc.Receive().(type) {
			case redis.Message:
				if data != nil {
					data(v.Channel, v.Pattern, v.Data)
				}
			case redis.Subscription:
				if stat != nil {
					stat(v.Channel, v.Kind, v.Count)
				}
			case error:
				return v
			}
		}
	}
	rc.P.Put(rc)
	return
}

func (p *pool) Eval(script string, keyCount int, keysArgs ...interface{}) (reply interface{}, err error) {
	rc, err := p.Get()
	if err == nil {
		reply, err = redis.NewScript(keyCount, script).Do(rc.C, keysArgs...)
	}
	p.Put(rc)
	return
}

func (p *pool) Close() {
	// 关闭无需获取锁,避免二次阻塞
	p.Foreach(func(c *conn) {
		c.C.Close()
	})
	return
}
