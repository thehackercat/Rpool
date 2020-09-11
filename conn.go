package pool

import "time"

type RConnection interface {
	GetUsage() int64
	GetPool() RPool
	GetVersion() int64
	GetClient() interface{}
	SetSession(interface{})
	GetSession() interface{}
	GetCreateTime() time.Time
	IncrUsage()
	Close() error
	Reset() error
	IsClosed() bool
	HandleClose(func() error)
	HandleReset(func() error)
	HandleIsClosed(func() bool)
}

type Connection struct {
	client     interface{}
	pool       RPool
	usage      int64
	session    interface{}
	_close     func() error
	reset      func() error
	isClosed   func() bool
	version    int64
	createTime time.Time
}

func (conn *Connection) GetUsage() int64 {
	return conn.usage
}

func (conn *Connection) GetPool() RPool {
	return conn.pool
}

func (conn *Connection) GetVersion() int64 {
	return conn.version
}

func (conn *Connection) GetClient() interface{} {
	return conn.client
}

func (conn *Connection) SetSession(session interface{}) {
	conn.session = session
}

func (conn *Connection) GetSession() interface{} {
	return conn.session
}

func (conn *Connection) GetCreateTime() time.Time {
	return conn.createTime
}

func (conn *Connection) IncrUsage() {
	conn.usage++
}

func (conn *Connection) Close() error {
	conn.ClearSession()
	if conn._close != nil {
		return conn._close()
	}
	return nil
}

func (conn *Connection) IsClosed() bool {
	if conn.isClosed != nil {
		return conn.isClosed()
	}
	return false
}

func (conn *Connection) Reset() error {
	conn.ClearSession()
	if conn.reset != nil {
		return conn.reset()
	}
	return nil
}

func (conn *Connection) ClearSession() {
	conn.session = nil
}
func (conn *Connection) HandleClose(_close func() error) {
	conn._close = _close
}

func (conn *Connection) HandleIsClosed(isClosed func() bool) {
	conn.isClosed = isClosed
}

func (conn *Connection) HandleReset(reset func() error) {
	conn.reset = reset
}

func NewConnection(pool RPool, client interface{}, version int64) *Connection {
	return &Connection{
		client:     client,
		pool:       pool,
		version:    version,
		createTime: time.Now(),
	}
}
