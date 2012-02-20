package pgsql

import (
	"bufio"
	"container/list"
	"errors"
	"fmt"
	"log"
	"runtime"
	"sync"
	"time"
)

const DEFAULT_IDLE_TIMEOUT = 300 // Seconds

type poolConn struct {
	*Conn
	atime time.Time // Time at which Conn is inserted into free list
}

type pool struct {
	params  string        // Params to create new Conn
	conns   *list.List    // List of available Conns
	max     int           // Maximum number of connections to create
	min     int           // min number of connections to create
	n       int           // Number of connections created
	cond    *sync.Cond    // Pool lock, and condition to signal when connection is released
	timeout time.Duration // Idle timeout period in seconds
	closed  bool
	Debug   bool // Set to true to print debug messages to stderr
}

func (p *pool) log(msg string) {
	if p.Debug {
		log.Println(time.Now().Format("2006-01-02 15:04:05"), msg)
	}
}

// A Pool manages a list of connections that can be safely used by multiple goroutines.
type Pool struct {
	// Subtle: Embed *pool struct so that timeoutCloser can operate on *pool
	// without preventing *Pool being garbage collected (and properly finalized).
	// See http://groups.google.com/group/golang-nuts/browse_thread/thread/d48b4d38e8fcc96f for discussion.
	*pool
}

// Close connections that have been idle for > p.timeout seconds.
func timeoutCloser(p *pool) {
	for p != nil && !p.closed {
		p.cond.L.Lock()
		now := time.Now()
		delay := p.timeout
		for p.conns.Len() > 0 {
			front := p.conns.Front()
			pc := front.Value.(poolConn)
			atime := pc.atime
			if (now.Sub(atime)) > p.timeout {
				pc.Conn.Close()
				p.conns.Remove(front)
				p.n--
				p.log("idle connection closed")
			} else {
				// Wait until first connection would timeout if it isn't used.
				delay = p.timeout - now.Sub(atime) + 1
				break
			}
		}
		// don't let the pool fall below the min
		for i := p.n; i < p.min; i++ {
			c, err := Connect(p.params, LogError)
			if err != nil {
				p.log("can't create connection")
			} else {
				p.conns.PushFront(poolConn{c, time.Now()})
				p.n++
			}
		}
		p.cond.L.Unlock()
		time.Sleep(delay * time.Second)
	}
	p.log("timeoutCloser finished")
}

// NewPool returns a new Pool that will create new connections on demand
// using connectParams, up to a maximum of maxConns outstanding connections.
// An error is returned if an initial connection cannot be created.
// Connections that have been idle for idleTimeout seconds will be automatically
// closed.
func NewPool(connectParams string, minConns, maxConns int, idleTimeout time.Duration) (p *Pool, err error) {
	if minConns < 1 {
		return nil, errors.New("minConns must be >= 1")
	}
	if maxConns < 1 {
		return nil, errors.New("maxConns must be >= 1")
	}
	if idleTimeout < 5 {
		return nil, errors.New("idleTimeout must be >= 5")
	}

	// Create initial connection to verify connectParams will work.
	c, err := Connect(connectParams, LogError)
	if err != nil {
		return
	}
	p = &Pool{
		&pool{
			params:  connectParams,
			conns:   list.New(),
			max:     maxConns,
			min:     minConns,
			n:       1,
			cond:    sync.NewCond(new(sync.Mutex)),
			timeout: idleTimeout,
		},
	}
	p.conns.PushFront(poolConn{c, time.Now()})

	for i := 0; i < minConns-1; i++ {
		// pre-fill the pool
		_c, err := Connect(connectParams, LogError)
		if err != nil {
			return nil, err
		}
		p.conns.PushFront(poolConn{_c, time.Now()})
		p.n++
	}

	go timeoutCloser(p.pool)
	runtime.SetFinalizer(p, (*Pool).close)
	return
}

// Acquire returns the next available connection, or returns an error if it
// failed to create a new connection.
// When an Acquired connection has been finished with, it should be returned
// to the pool via Release.
func (p *Pool) Acquire() (c *Conn, err error) {
	p.cond.L.Lock()
	defer p.cond.L.Unlock()
	if p.closed {
		return nil, errors.New("pool is closed")
	}
	if p.conns.Len() > 0 {
		c = p.conns.Remove(p.conns.Front()).(poolConn).Conn
	} else if p.conns.Len() == 0 && p.n < p.max {
		c, err = Connect(p.params, LogError)
		if err != nil {
			return
		}
		p.n++
		if p.Debug {
			p.log(fmt.Sprintf("connection %d created", p.n))
		}
	} else { // p.conns.Len() == 0 && p.n == p.max
		for p.conns.Len() == 0 {
			p.cond.Wait()
		}
		c = p.conns.Remove(p.conns.Front()).(poolConn).Conn
	}
	if p.Debug {
		p.log(fmt.Sprintf("connection acquired: %d idle, %d unused", p.conns.Len(), p.max-p.n))
	}
	return c, nil
}

// Release returns the previously Acquired connection to the list of available connections.
func (p *Pool) Release(c *Conn) {
	p.cond.L.Lock()
	defer p.cond.L.Unlock()
	if !p.closed {
		// reset the connection
		c.reader = bufio.NewReader(c.tcpConn)
		c.writer = bufio.NewWriter(c.tcpConn)
		c.state = readyState{}

		// push back to the queue
		p.conns.PushBack(poolConn{c, time.Now()})
		if p.Debug {
			p.log(fmt.Sprintf("connection released: %d idle, %d unused", p.conns.Len(), p.max-p.n))
		}
		p.cond.Signal()
	}
}

func (p *Pool) close() {
	if p != nil {
		nConns := p.conns.Len()
		for p.conns.Len() > 0 {
			p.conns.Remove(p.conns.Front()).(poolConn).Close()
		}
		p.n -= nConns
		p.closed = true
		runtime.SetFinalizer(p, nil)
		p.log("close finished")
	}
}

// Close closes any available connections and prevents the Acquiring of any new connections.
// It returns an error if there are any outstanding connections remaining.
func (p *Pool) Close() error {
	p.cond.L.Lock()
	defer p.cond.L.Unlock()
	if !p.closed {
		p.close()
		if p.n > 0 {
			return errors.New(fmt.Sprintf("pool closed but %d connections in use", p.n))
		}
	}
	return nil
}
