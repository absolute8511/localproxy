package proxy

import (
	"errors"
	"fmt"
	"github.com/absolute8511/proxymodule/util"
	"io"
	"net"
	"sync/atomic"
	"time"
)

const (
	RSP_BUF_SIZE     = 1024
	BUF_MAX_NUM      = 200
	MAX_CONN_NUM     = 100
	SHRINK_THRESHOLD = 10
	SHRINK_NUM       = 2
	CONN_TIMEOUT     = 3
)

type ProxyConnData struct {
	conn        *net.TCPConn
	rspBuf      chan *util.FixedBuffer
	closing     int32
	holding     chan struct{}
	closeNotify chan struct{}
}

func NewProxyConnData(c *net.TCPConn) *ProxyConnData {
	return &ProxyConnData{
		conn:        c,
		rspBuf:      make(chan *util.FixedBuffer, 10),
		closing:     0,
		holding:     make(chan struct{}, 1),
		closeNotify: make(chan struct{}),
	}
}

func (self *ProxyConnData) Close() {
	if atomic.CompareAndSwapInt32(&self.closing, 0, 1) {
		close(self.closeNotify)
	}
	self.conn.CloseRead()
}

type TCPConnPool struct {
	pid      string
	address  string
	lastUsed time.Time

	connChan  chan *ProxyConnData
	used      int32
	bufPool   *util.FixedBufferPool
	useRspBuf bool
}

// if use response buffer, the pool will manager the read from remote and
// the client should read from the buffer. Otherwise, the client can read
// from remote directly.
func NewTCPConnPool(userspbuf bool, pid, address string, p *util.FixedBufferPool) *TCPConnPool {
	ret := &TCPConnPool{
		pid:       pid,
		address:   address,
		lastUsed:  time.Now(),
		connChan:  make(chan *ProxyConnData, MAX_CONN_NUM),
		used:      0,
		useRspBuf: userspbuf,
	}
	ret.bufPool = p
	if ret.bufPool == nil {
		ret.bufPool = util.NewFixedBufferPool(RSP_BUF_SIZE, BUF_MAX_NUM, false)
	}
	return ret
}

func (self *TCPConnPool) readRspFromRemote(remote *ProxyConnData) {
	//readbuf := make([]byte, RSP_BUF_SIZE)
	bytesNum := 0
	for {
		data := self.bufPool.GetBuffer()
		n, err := remote.conn.Read(data.GetRawBuffer())
		if err != nil {
			if err != io.EOF {
				glog.Errorf("!!! read error from remote: %v", err)
			}
			self.bufPool.PutBuffer(data)
			break
		}
		if n == 0 {
			glog.Infof("read 0 from remote without error.")
			self.bufPool.PutBuffer(data)
			continue
		}
		bytesNum += n
		data.UseBuffer(int32(n))
		select {
		case remote.rspBuf <- data:
		case <-remote.closeNotify:
			self.bufPool.PutBuffer(data)
			break
		}
	}
	if atomic.CompareAndSwapInt32(&remote.closing, 0, 1) {
		close(remote.closeNotify)
	}
	glog.Debugf("remote proxy connection %v-%v finished, total : %v bytes.",
		self.address, remote.conn.LocalAddr().String(), bytesNum)
	// we should free the buffer from pool
	select {
	case remote.holding <- struct{}{}:
		finished := false
		for !finished {
			select {
			case b := <-remote.rspBuf:
				self.bufPool.PutBuffer(b)
			default:
				if len(remote.rspBuf) == 0 {
					finished = true
				}
			}
		}
		<-remote.holding
	}
	close(remote.rspBuf)
	// the local may left data to send to remote,
	// so only close read to let the data send last time.
	remote.conn.CloseRead()
}

func (self *TCPConnPool) waitConn(t time.Duration) (bool, *ProxyConnData, error) {
	isNewConn := false
	wt := time.NewTimer(t)
	for {
		select {
		case c := <-self.connChan:
			if atomic.LoadInt32(&c.closing) == 1 {
				glog.Debugf("ignore closing connection while acquire. %v : %v\n", self.address, c.conn.LocalAddr().String())
				continue
			}
			if len(c.rspBuf) > 0 {
				glog.Errorf("found a not empty rspbuf: %v, %v, %v", len(c.rspBuf), self.address, self.pid)
				c.Close()
				continue
			}
			c.holding <- struct{}{}
			atomic.AddInt32(&self.used, 1)
			return isNewConn, c, nil
		case <-wt.C:
			wt.Stop()
			return isNewConn, nil, errors.New("time out")
		}
	}
}

func (self *TCPConnPool) AcquireConn() (bool, *ProxyConnData, error) {
	isNewConn := false
	for {
		select {
		case c := <-self.connChan:
			if atomic.LoadInt32(&c.closing) == 1 {
				glog.Debugf("ignore closing connection while acquire. %v : %v\n", self.address, c.conn.LocalAddr().String())
				continue
			}
			if len(c.rspBuf) > 0 {
				glog.Errorf("found a not empty rspbuf: %v, %v, %v", len(c.rspBuf), self.address, self.pid)
				c.Close()
				continue
				//return isNewConn, c, errors.New("remote state not ok.")
			}
			c.holding <- struct{}{}
			atomic.AddInt32(&self.used, 1)
			return isNewConn, c, nil
		default:
			// new conn
			if atomic.LoadInt32(&self.used) > MAX_CONN_NUM*2 {
				glog.Warningf("Too many connections :%v, %v, used: %v", self.address, self.pid, self.used)
				isNewConn, c, err := self.waitConn(time.Second)
				if err == nil && c != nil {
					return isNewConn, c, err
				}
			}
			proxyconn := NewProxyConnData(nil)
			// new conn
			c, err := net.DialTimeout("tcp", self.address, time.Duration(10)*time.Second)
			if err != nil {
				glog.Infof("Connect to %v failed: %v\n", self.address, err.Error())
				return isNewConn, nil, err
			}
			isNewConn = true
			proxyconn.conn = c.(*net.TCPConn)
			glog.Debugf("new connection to remote %v.\n", self.address)
			if self.useRspBuf {
				go self.readRspFromRemote(proxyconn)
			}
			proxyconn.holding <- struct{}{}
			atomic.AddInt32(&self.used, 1)
			return isNewConn, proxyconn, nil
		}
	}
}

func (self *TCPConnPool) ReleaseConn(p *ProxyConnData) {
	//glog.V(3).Infof("== connection begin release: %v, %v, %v.", self.address, self.pid, id)
	if len(p.rspBuf) > 0 {
		glog.Warningf("connection rspBuf left data : %v. %v", len(p.rspBuf), self.address)
		atomic.StoreInt32(&p.closing, 1)
	}

	select {
	case <-p.holding:
		// release hold
	default:
		// already released
		return
	}
	atomic.AddInt32(&self.used, -1)
	self.lastUsed = time.Now()
	if atomic.LoadInt32(&p.closing) == 1 {
		p.Close()
		return
	}

	select {
	case self.connChan <- p:
		// release to pool
		glog.Debugf("== connection ready for reuse: %v, %v.", self.address, self.pid)
	default:
		// pool is full, just discard connection
		glog.Debugf("== connection discard : %v, %v.", self.address, self.pid)
		p.Close()
	}
}

func (self *TCPConnPool) DestroyRemoteConn(proxyconn *ProxyConnData) {
	atomic.StoreInt32(&proxyconn.closing, 1)
	select {
	case <-proxyconn.holding:
		// release hold
	default:
		// already released
		return
	}

	atomic.AddInt32(&self.used, -1)
	self.lastUsed = time.Now()
	if len(proxyconn.rspBuf) > 0 {
		glog.Infof("connection rspBuf left data : %v.", len(proxyconn.rspBuf))
	}
	glog.Debugf("====== connection destroyed: %v, %v.", self.address, self.pid)
	proxyconn.Close()
}

func (self *TCPConnPool) IsUnusedSince(t time.Time) bool {
	if self.lastUsed.After(t) {
		return false
	}
	if atomic.LoadInt32(&self.used) == 0 {
		return true
	}
	return false
}

func (self *TCPConnPool) ShrinkPool() {
	if len(self.connChan) < SHRINK_THRESHOLD+SHRINK_NUM {
		return
	}
	for i := 0; i < SHRINK_NUM; i++ {
		select {
		case c := <-self.connChan:
			c.Close()
		default:
			return
		}
	}
}

func (self *TCPConnPool) CleanPool() {
	for {
		select {
		case c := <-self.connChan:
			c.Close()
		default:
			glog.Infof("pool cleaned: %v, %v", self.address, self.pid)
			return
		}
	}
	// maybe wait for all outside used remote to close.
}

func (self *TCPConnPool) IsEmpty() bool {
	return len(self.connChan) == 0 && atomic.LoadInt32(&self.used) <= 0
}

func (self *TCPConnPool) StatusString() string {
	statusStr := fmt.Sprintf("  %v : used - %v, free - %v, lastUsed:%v\n", self.address, self.used,
		len(self.connChan), self.lastUsed)
	return statusStr
}
