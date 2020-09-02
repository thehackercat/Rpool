package pool

import (
	"container/ring"
	"sync"
)

type Ring struct {
	imp *ring.Ring
	sync.Mutex
	count int
}

func (r *Ring) Len() int {
	return r.count
}

func (r *Ring) Push(conn RConnection) {
	_ring := &ring.Ring{Value: conn}
	r.Lock()
	defer r.Unlock()
	if r.count == 0 {
		r.imp = _ring
	} else {
		r.imp.Link(_ring)
	}
	r.count += 1
}

func (r *Ring) Pop() (conn RConnection) {
	r.Lock()
	defer r.Unlock()
	if r.count >= 1 {
		conn = r.imp.Value.(RConnection)
		if r.count == 1 {
			r.imp = nil
		} else {
			t := r.imp.Prev()
			t.Unlink(1)
			r.imp = t
		}
		r.count -= 1
	}
	return
}
