package pool

import (
	"errors"
	"fmt"
	"math/rand"
	"sync/atomic"
	"time"
)

var (
	ErrExceedMaxPoolLimit = errors.New("exceed max pool limit")
)

type RPool interface {
	GetDomain() string
	Acquire() (RConnection, error)
	Release(RConnection)
	GetPayload() interface{}
	SetPayload(interface{})
	GetDescription() string
	SetDescription(string)
	GetVersion() int64
	IncrVersion()
	SetConnExpire(time.Duration)
	SetConnMaxUsage(int64)
	SetDropConnThreshold(float64)
	Len() int
	Capacity() int
	SetCapacity(int)
}

type Pool struct {
	domain            string
	free              *Ring
	limit             int
	usage             int32
	description       string
	payload           interface{}
	version           int64
	factory           func(RPool) (RConnection, error)
	connMaxUsage      int64
	connExpire        time.Duration
	connDropThreshold float64
	statistics        *Statistics
}

type Statistics struct {
	ReleaseCount int64
	AcquireCount int64
}

func (pool *Pool) Capacity() int {
	return pool.limit
}

func (pool *Pool) Len() int {
	return pool.free.Len()
}

func (pool *Pool) SetCapacity(limit int) {
	pool.limit = limit
}

func (pool *Pool) GetDomain() string {
	return pool.domain
}

func (pool *Pool) SetConnExpire(expire time.Duration) {
	pool.connExpire = expire
}

func (pool *Pool) SetConnMaxUsage(usage int64) {
	pool.connMaxUsage = usage
}

func (pool *Pool) SetDropConnThreshold(threshold float64) {
	pool.connDropThreshold = threshold
}

func (pool *Pool) GetDescription() string {
	return pool.description
}

func (pool *Pool) SetDescription(description string) {
	pool.description = description
}

func (pool *Pool) GetPayload() interface{} {
	return pool.payload
}
func (pool *Pool) SetPayload(payload interface{}) {
	pool.payload = payload
}

func (pool *Pool) IncrVersion() {
	pool.version++
}

func (pool *Pool) GetVersion() int64 {
	return pool.version
}

func (pool *Pool) Acquire() (conn RConnection, err error) {
	if conn = pool.free.Pop(); conn == nil {
		if atomic.LoadInt32(&pool.usage) >= int32(pool.limit) {
			err = ErrExceedMaxPoolLimit
		} else if conn, err = pool.factory(pool); err != nil {
			fmt.Printf("failed create new connection", err.Error())
			return
		}
		if err == nil {
			atomic.AddInt32(&pool.usage, 1)
			atomic.AddInt64(&pool.statistics.AcquireCount, 1)
		}
	} else {
		if conn.GetVersion() != pool.GetVersion() || conn.IsClosed() {
			conn.Close()
			return pool.Acquire()
		} else if pool.connExpire != 0 {
			threshold := time.Duration(rand.Int63n(int64(pool.connExpire.Seconds()*pool.connDropThreshold))) * time.Second
			if time.Now().Sub(conn.GetCreateTime()) > (pool.connExpire - threshold) {
				conn.Close()
				return pool.Acquire()
			}
		}
		atomic.AddInt32(&pool.usage, 1)
		atomic.AddInt64(&pool.statistics.AcquireCount, 1)
	}
	return
}

func (pool *Pool) Release(conn RConnection) {
	if conn != nil {
		conn.IncrUsage()
		if !conn.IsClosed() {
			conn.Reset()
			if pool.connMaxUsage != 0 {
				threshold := rand.Int63n(int64(float64(pool.connMaxUsage) * pool.connDropThreshold))
				if conn.GetUsage() > (pool.connMaxUsage - threshold) {
					conn.Close()
				} else {
					pool.free.Push(conn)
				}
			} else {
				pool.free.Push(conn)
			}
		}
		atomic.AddInt32(&pool.usage, -1)
		atomic.AddInt64(&pool.statistics.ReleaseCount, 1)
	}
}

func NewPool(domain string, limit int, factory func(RPool) (RConnection, error)) RPool {
	p := &Pool{
		domain:            domain,
		free:              NewRing(),
		limit:             limit,
		factory:           factory,
		connDropThreshold: 0.1,
		statistics:        &Statistics{},
	}
	return p
}
