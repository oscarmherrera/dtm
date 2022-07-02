package pooler

import (
	"errors"
	"sync/atomic"
)

// ErrServersNotExists is the error that servers dose not exists
var ErrServersNotExists = errors.New("servers dose not exist")

// RoundRobin is an interface for representing round-robin balancing.
type RoundRobin interface {
	Next() *string
}

type roundrobin struct {
	RoundRobin
	servers []*string
	next    uint32
}

// New returns RoundRobin implementation(*roundrobin).
func NewRoundRobin(svrs ...*string) (RoundRobin, error) {
	if len(svrs) == 0 {
		return nil, ErrServersNotExists
	}

	return &roundrobin{
		servers: svrs,
	}, nil
}

// Next returns next address
func (r *roundrobin) Next() *string {
	n := atomic.AddUint32(&r.next, 1)
	return r.servers[(int(n)-1)%len(r.servers)]
}
