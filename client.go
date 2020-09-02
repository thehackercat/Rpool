package pool

type Client struct {
	Service string
	Pool    IPool
}

func (c *Client) GetClient() (client interface{}, df func(), err error) {
	var conn RConnection
	if conn, err = c.Pool.Acquire(); err == nil {
		client = conn.GetClient()
		df = func() {
			c.Pool.Release(conn)
		}
	}
	return
}
