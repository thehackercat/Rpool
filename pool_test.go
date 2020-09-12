package pool

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

type TestClient struct {
	closed bool
}

func (client *TestClient) Close() error {
	client.closed = true
	return nil
}

func (client *TestClient) IsClosed() bool {
	return client.closed
}

func testFactory(p RPool) (conn RConnection, err error) {
	client := &TestClient{}
	version := p.GetVersion()
	conn = NewConnection(p, client, version)
	conn.HandleClose(client.Close)
	conn.HandleIsClosed(client.IsClosed)
	return
}

func TestPool(t *testing.T) {
	Convey("Check Pool/Connecton", t, func() {
		Convey("Check basic attribute", func() {
			pool := NewPool("test", 10, testFactory)
			So(pool.GetDomain(), ShouldEqual, "test")
			So(pool.GetVersion(), ShouldEqual, 0)
			So(pool.Len(), ShouldEqual, 0)
			So(pool.Capacity(), ShouldEqual, 10)
		})
		Convey("Check acquire & release", func() {
			Convey("Check basic acquire & release", func() {
				pool := NewPool("test", 10, testFactory)
				So(pool.Len(), ShouldEqual, 0)
				conn, err := pool.Acquire()
				So(err, ShouldBeNil)
				So(conn, ShouldNotBeNil)
				client := conn.GetClient().(*TestClient)
				So(client, ShouldNotBeNil)
				So(client.IsClosed(), ShouldBeFalse)
				So(pool.Len(), ShouldEqual, 0)
				pool.Release(conn)
				So(pool.Len(), ShouldEqual, 1)
			})
			Convey("Close client before release it", func() {
				pool := NewPool("test", 10, testFactory)
				conn, err := pool.Acquire()
				So(err, ShouldBeNil)
				So(conn, ShouldNotBeNil)
				client := conn.GetClient().(*TestClient)
				client.Close()
				So(pool.Len(), ShouldEqual, 0)
				pool.Release(conn)
				So(pool.Len(), ShouldEqual, 0)
			})
			Convey("acquire before release", func() {
				pool := NewPool("test", 1, testFactory)
				conn1, err1 := pool.Acquire()
				So(err1, ShouldBeNil)
				So(conn1, ShouldNotBeNil)

				conn2, err2 := pool.Acquire()
				So(err2, ShouldEqual, ErrExceedMaxPoolLimit)
				So(conn2, ShouldBeNil)

				pool.Release(conn2)

				conn3, err3 := pool.Acquire()
				So(err3, ShouldEqual, ErrExceedMaxPoolLimit)
				So(conn3, ShouldBeNil)

				pool.Release(conn1)

				conn4, err4 := pool.Acquire()
				So(err4, ShouldBeNil)
				So(conn4, ShouldNotBeNil)
			})
		})
	})
}
