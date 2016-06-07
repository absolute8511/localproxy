package proxy

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"github.com/absolute8511/grace/gracenet"
	"github.com/absolute8511/proxymodule/common"
	"github.com/absolute8511/proxymodule/util"
	"github.com/gorilla/mux"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	npprof "net/http/pprof"
	"os"
	"runtime/pprof"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

var glog = common.NewLevelLogger(1, nil)

const (
	errClosed            = "use of closed network connection"
	TMP_PROXY_PARAM_PATH = "/tmp/proxyparams.tmp"
)

var (
	didInherit = os.Getenv("LISTEN_FDS") != ""
	ppid       = os.Getppid()
)

type ProxyParam struct {
	PID        string
	RemoteAddr string
	Protocol   string
	ProxyAddr  string
	// this is used to send the data to the server while
	// reconnect to the server.
	// Since the reconnect is hidden from client side, this will be
	// useful if the client-server have some specific logical when
	// the client first connected and need some auth to server.

	InitSendData string
}

func GetUnixDomainPath(param *ProxyParam) string {
	var tmpbuf bytes.Buffer
	tmpbuf.WriteString("/tmp/")
	tmpbuf.WriteString(param.PID)
	tmpbuf.WriteString("-")
	tmpbuf.WriteString(param.RemoteAddr)
	tmpbuf.WriteString(".sock")
	return tmpbuf.String()
}

type ProxyListenerData struct {
	ProxyParam
	LastUsed  time.Time
	AutoClean bool
}

type LocalProxy struct {
	sync.Mutex
	l        net.Listener
	stopping int32
	// pid -> adress -> TCPConnPool
	remoteServers map[string]map[string]*TCPConnPool
	// pid -> path -> Listeners
	localListeners map[string]map[string]net.Listener
	proxyParams    map[string]map[string]ProxyListenerData
	//
	wg        sync.WaitGroup
	graceNet  *gracenet.Net
	scheduler *util.TimerScheduler
}

func NewLocalProxy() *LocalProxy {
	return &LocalProxy{
		stopping:       0,
		remoteServers:  make(map[string]map[string]*TCPConnPool),
		localListeners: make(map[string]map[string]net.Listener),
		proxyParams:    make(map[string]map[string]ProxyListenerData),
		graceNet:       &gracenet.Net{},
		scheduler:      util.NewTimerScheduler(),
	}
}

func (self *LocalProxy) getConnPool(proxyParam *ProxyParam) *TCPConnPool {
	self.Lock()
	pidpool, ok := self.remoteServers[proxyParam.PID]
	if !ok {
		// new connection pool to server.
		pidpool = make(map[string]*TCPConnPool)
		self.remoteServers[proxyParam.PID] = pidpool
	}
	tcppool, ok := pidpool[proxyParam.RemoteAddr]
	if !ok {
		tcppool = NewTCPConnPool(false, proxyParam.PID, proxyParam.RemoteAddr, nil)
		pidpool[proxyParam.RemoteAddr] = tcppool
	}
	self.Unlock()
	return tcppool
}

func (self *LocalProxy) getServerConn(proxyParam *ProxyParam) (bool, *ProxyConnData, error) {
	tcppool := self.getConnPool(proxyParam)
	return tcppool.AcquireConn()
}

func (self *LocalProxy) destroyRemoteConn(proxyParam *ProxyParam) {
	self.Lock()
	pidpool, ok := self.remoteServers[proxyParam.PID]
	if !ok {
		self.Unlock()
		return
	}
	tcppool, ok := pidpool[proxyParam.RemoteAddr]
	self.Unlock()
	if ok {
		tcppool.CleanPool()
	}
}

func (self *LocalProxy) stopAcceptProxy(proxyParam *ProxyParam) {
	path := GetUnixDomainPath(proxyParam)
	self.Lock()
	defer self.Unlock()
	if listeners, ok := self.localListeners[proxyParam.PID]; ok {
		if l, ok := listeners[path]; ok {
			delete(listeners, path)
			delete(self.proxyParams[proxyParam.PID], path)
			if len(listeners) == 0 {
				delete(self.localListeners, proxyParam.PID)
				delete(self.proxyParams, proxyParam.PID)
			}
			self.graceNet.Close(l)
		}
	}
	if p, ok := self.remoteServers[proxyParam.PID]; ok {
		if pool, ok := p[proxyParam.RemoteAddr]; ok {
			pool.CleanPool()
		}
	}
}

func (self *LocalProxy) stopAllPIDAcceptProxy(pid string) {
	var poolList map[string]*TCPConnPool
	self.Lock()
	if pidpool, ok := self.remoteServers[pid]; ok {
		poolList = pidpool
		self.remoteServers[pid] = make(map[string]*TCPConnPool)
	}
	listeners := self.localListeners[pid]
	self.localListeners[pid] = make(map[string]net.Listener)
	self.proxyParams[pid] = make(map[string]ProxyListenerData)
	self.Unlock()
	for _, pool := range poolList {
		pool.CleanPool()
	}
	for _, l := range listeners {
		self.graceNet.Close(l)
	}
}

func (self *LocalProxy) checkUnusedConnPool() {
	self.Lock()
	expireTime := time.Now().Add(-1 * time.Minute * 10)
	for pid, pidpool := range self.remoteServers {
		isAllEmpty := true
		for _, tcppool := range pidpool {
			tcppool.ShrinkPool()
			if tcppool.IsUnusedSince(expireTime) {
				tcppool.CleanPool()
			} else {
				isAllEmpty = false
			}
		}
		if isAllEmpty {
			glog.Infof("PID pool cleaned: %v", pid)
			if listeners, ok := self.proxyParams[pid]; ok {
				for k, l := range listeners {
					if l.AutoClean && !l.LastUsed.After(expireTime) {
						self.graceNet.Close(self.localListeners[pid][k])
						delete(self.localListeners[pid], k)
						delete(self.proxyParams[pid], k)
					}
				}
				if len(listeners) == 0 {
					delete(self.localListeners, pid)
					delete(self.proxyParams, pid)
				}
			}
		}
	}
	self.Unlock()
}

func (self *LocalProxy) StartLocalProxy(port uint16) error {
	addr := "127.0.0.1:" + strconv.Itoa(int(port))
	var err error
	self.l, err = self.graceNet.Listen("tcp", addr)

	if err != nil {
		glog.Errorf("err: %v", err)
		return err
	}
	glog.Infof("LocalProxy listen on:%v.", self.l.Addr())
	globalMux := http.NewServeMux()
	apiRouter := mux.NewRouter()

	apiRouter.HandleFunc("/api/get-proxy-conn", self.HandleGetProxyConn)
	apiRouter.HandleFunc("/api/close-proxy-conn", self.HandleCloseProxyConn)
	apiRouter.HandleFunc("/api/destroy-proxy", self.HandleDestroyProxyConn)
	apiRouter.HandleFunc("/api/destroy-all", self.HandleDestroyAll)
	apiRouter.HandleFunc("/api/status", self.HandleStatus)
	apiRouter.HandleFunc("/api/debug-status", self.HandleDebugStatus)
	apiRouter.HandleFunc("/api/setlog-level/{level:.*}", self.HandleSetLogLevel)

	globalMux.Handle("/api/", apiRouter)
	globalMux.Handle("/debug/pprof", http.HandlerFunc(npprof.Index))
	globalMux.Handle("/debug/pprof/cmdline", http.HandlerFunc(npprof.Cmdline))
	globalMux.Handle("/debug/pprof/profile", http.HandlerFunc(npprof.Profile))
	globalMux.Handle("/debug/pprof/symbol", http.HandlerFunc(npprof.Symbol))

	self.StartInheritedProxy()

	// clean unused connection period
	self.scheduler.ScheduleJob(self.checkUnusedConnPool, time.Minute, true)

	return http.Serve(self.l, globalMux)
}

func (self *LocalProxy) Stop() {
	if !atomic.CompareAndSwapInt32(&self.stopping, 0, 1) {
		glog.Infof("already stopping")
		return
	}
	self.Lock()
	glog.Infof("stopping the local proxy.")
	ll := self.localListeners
	self.localListeners = make(map[string]map[string]net.Listener)
	self.proxyParams = make(map[string]map[string]ProxyListenerData)
	self.Unlock()

	for _, listeners := range ll {
		for _, l := range listeners {
			glog.Debugf("closing listener:%v", l.Addr().String())
			l.Close()
		}
	}
	// we need trigger the remote close if the client connection
	// did not close for a long time.
	self.scheduler.ScheduleJob(
		func() {
			self.Lock()
			glog.Infof("timeout for waiting client close.")
			for _, servers := range self.remoteServers {
				for _, tcppool := range servers {
					tcppool.CleanPool()
				}
			}
			self.remoteServers = make(map[string]map[string]*TCPConnPool)
			self.Unlock()
		}, time.Second*5, false)

	self.wg.Wait()
	glog.Infof("local proxy stopped.")
	self.l.Close()
	glog.Flush()
	self.scheduler.StopScheduler()
}

func (self *LocalProxy) StartInheritedProxy() error {

	f, err := os.Open(TMP_PROXY_PARAM_PATH)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		glog.Errorf("start inherited proxy failed: %v", err)
		return err
	}
	d, err := ioutil.ReadAll(f)
	if err != nil {
		glog.Errorf("read inherited data failed: %v", err)
		return err
	}
	err = json.Unmarshal(d, &self.proxyParams)
	if err != nil {
		glog.Errorf("error unmarshal proxy data: %v", err)
		return err
	}
	count := 0
	for pid, params := range self.proxyParams {
		count += len(params)
		for path, param := range params {
			if _, ok := self.localListeners[pid]; !ok {
				self.localListeners[pid] = make(map[string]net.Listener)
			}
			var l net.Listener
			if strings.ToUpper(param.Protocol) == "HTTP" {
				l, err = self.graceNet.Listen("tcp", param.ProxyAddr)
			} else {
				l, err = self.graceNet.Listen("unix", param.ProxyAddr)
			}
			if err != nil {
				glog.Errorf("err: %v", err)
				delete(self.proxyParams[pid], path)
				continue
			}
			glog.Infof("proxy listen on: %v.", l.Addr().String())
			self.localListeners[pid][path] = l
			self.wg.Add(1)
			go self.doAccept(&param.ProxyParam, l)
		}
	}
	glog.Infof("inherited proxy: %v", count)
	os.Remove(TMP_PROXY_PARAM_PATH)
	// Some useful logging.
	if didInherit {
		if ppid == 1 {
			glog.Infof("Listening on init activated.")
		} else {
			const msg = "Graceful handoff with new pid %d and old pid %d"
			glog.Infof(msg, os.Getpid(), ppid)
		}
	} else {
		const msg = "Serving with pid %d"
		glog.Infof(msg, os.Getpid())
	}

	// Close the parent if we inherited and it wasn't init that started us.
	if didInherit && ppid != 1 {
		if err := syscall.Kill(ppid, syscall.SIGTERM); err != nil {
			return errors.New("failed to close parent: " + err.Error())
		}
	}
	return nil
}

func (self *LocalProxy) Restart() {
	// save running proxy to file
	json, _ := json.Marshal(self.proxyParams)
	f, err := os.Create(TMP_PROXY_PARAM_PATH)
	if err != nil {
		glog.Errorf("open file failed: %v", err)
		return
	}
	f.Write(json)

	if _, err := self.graceNet.StartProcess(); err != nil {
		//self.errors <- err
		glog.Errorf("failed to restart the proxy: %v", err)
	}
}

func getProxyParam(r *http.Request) (*ProxyParam, error) {
	var proxyParam ProxyParam
	err := json.NewDecoder(r.Body).Decode(&proxyParam)
	if err != nil {
		glog.Infof("proxy param error: %v\n", err)
		return nil, err
	}
	return &proxyParam, nil
}

func (self *LocalProxy) HandleStatus(w http.ResponseWriter, r *http.Request) {
	self.Lock()
	var statusStr string
	for pid, pidpool := range self.remoteServers {
		statusStr += pid + " : \n"
		for _, tcppool := range pidpool {
			statusStr += tcppool.StatusString()
		}
	}
	self.Unlock()
	w.Write([]byte(statusStr))
}

func (self *LocalProxy) HandleSetLogLevel(w http.ResponseWriter, r *http.Request) {
	level := mux.Vars(r)["level"]
	if level == "" {
		http.Error(w, "level is need", http.StatusBadRequest)
		return
	}
	self.Lock()
	flag.Set("v", level)
	self.Unlock()
}

func (self *LocalProxy) HandleDebugStatus(w http.ResponseWriter, r *http.Request) {
	var statusStr string
	self.Lock()
	for pid, pidpool := range self.remoteServers {
		statusStr += pid + " : \n"
		for _, tcppool := range pidpool {
			statusStr += tcppool.StatusString()
		}
	}
	self.Unlock()
	glog.Infof("Begin pprof dump ======")
	pprof.Lookup("heap").WriteTo(os.Stdout, 2)
	pprof.Lookup("threadcreate").WriteTo(os.Stdout, 2)
	pprof.Lookup("block").WriteTo(os.Stdout, 2)
	pprof.Lookup("goroutine").WriteTo(os.Stdout, 2)
	glog.Infof("End pprof dump ======")
	w.Write([]byte(statusStr))
}

// If the remote will be not used anymore, the local listener can be deleted.
func (self *LocalProxy) HandleDestroyProxyConn(w http.ResponseWriter, r *http.Request) {
	param, err := getProxyParam(r)
	//go self.checkUnusedConnPool()
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	self.destroyRemoteConn(param)
	self.stopAcceptProxy(param)
}

// proxy client could call the close to avoid the connection data mismatch.
// If any timeout or error on the client the connection should be closed.
// If the client finished normally, the client can just close the socket and the connection
//  on the remote will be reused.
func (self *LocalProxy) HandleCloseProxyConn(w http.ResponseWriter, r *http.Request) {
	param, err := getProxyParam(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	self.destroyRemoteConn(param)
}

func (self *LocalProxy) HandleDestroyAll(w http.ResponseWriter, r *http.Request) {
	param, err := getProxyParam(r)
	go self.checkUnusedConnPool()
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	self.stopAllPIDAcceptProxy(param.PID)
}

func (self *LocalProxy) GetProxy(pid, protocol, remote, initData string) (string, error) {
	var param ProxyParam
	param.PID = pid
	param.Protocol = protocol
	param.RemoteAddr = remote
	param.InitSendData = initData
	return self.getProxy(&param, false)
}

func (self *LocalProxy) getProxy(param *ProxyParam, clean bool) (string, error) {
	path := GetUnixDomainPath(param)
	self.Lock()
	defer self.Unlock()
	if listeners, ok := self.localListeners[param.PID]; ok {
		if l, ok := listeners[path]; ok {
			pp := self.proxyParams[param.PID][path]
			pp.LastUsed = time.Now()
			self.proxyParams[param.PID][path] = pp
			return l.Addr().String(), nil
		}
	} else {
		self.localListeners[param.PID] = make(map[string]net.Listener)
		self.proxyParams[param.PID] = make(map[string]ProxyListenerData)
	}
	var l net.Listener
	var err error
	if strings.ToUpper(param.Protocol) == "HTTP" {
		l, err = self.graceNet.Listen("tcp", "127.0.0.1:0")
	} else {
		l, err = self.graceNet.Listen("unix", path)
	}
	if err != nil {
		glog.Errorf("err: %v", err)
		return "", err
	}
	glog.Infof("proxy listen on: %v.", l.Addr().String())
	param.ProxyAddr = l.Addr().String()
	self.localListeners[param.PID][path] = l
	var pp ProxyListenerData
	pp.ProxyParam = *param
	pp.AutoClean = clean
	pp.LastUsed = time.Now()
	self.proxyParams[param.PID][path] = pp
	self.wg.Add(1)
	go self.doAccept(param, l)

	return l.Addr().String(), nil
}

func (self *LocalProxy) HandleGetProxyConn(w http.ResponseWriter, r *http.Request) {
	param, err := getProxyParam(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	addr, err := self.getProxy(param, true)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Write([]byte(addr))
}

func copyBytesToRemote(local net.Conn, remote *net.TCPConn) bool {
	should_close_remote := false
	if bytesNum, err := io.Copy(remote, local); err != nil {
		glog.Infof("proxy IO error: %v.", err)
		should_close_remote = true
	} else {
		_ = bytesNum
		glog.Debugf("proxy connection send to remote finished, total : %v bytes.", bytesNum)
	}
	// we may have some left data to send, so we only close read here.
	if c, ok := local.(*net.UnixConn); ok {
		c.CloseRead()
	} else if c, ok := local.(*net.TCPConn); ok {
		c.CloseRead()
	} else {
		local.Close()
	}
	remote.SetReadDeadline(time.Now())
	return should_close_remote
}

func copyRspFromRemoteBuf(pool *util.FixedBufferPool, remoteConn *net.TCPConn,
	local net.Conn) bool {
	should_close_remote := false
	bytesNum := 0
	for {
		data := pool.GetBuffer()
		err := func() error {
			defer pool.PutBuffer(data)
			n, err := remoteConn.Read(data.GetRawBuffer())
			if err != nil {
				netErr, ok := err.(net.Error)
				if ok && netErr.Timeout() {
					glog.Debugf("notify closed by client: %v, read from remote bytes %v.", remoteConn.RemoteAddr().String(), bytesNum)
				} else {
					if err != io.EOF {
						glog.Errorf("!!! read error from remote: %v", err)
					}
					should_close_remote = true
					glog.Debugf("remote proxy connection %v-%v finished, total : %v bytes.",
						remoteConn.RemoteAddr().String(), remoteConn.LocalAddr().String(), bytesNum)
				}
				return err
			}
			if n == 0 {
				glog.Infof("read 0 from remote without error.")
				return nil
			}
			bytesNum += n
			data.UseBuffer(int32(n))

			_, err = local.Write(data.GetUsedBuffer())
			if err != nil {
				should_close_remote = true
			}
			return err
		}()
		if err != nil {
			break
		}
	}
	local.Close()
	return should_close_remote
}

func proxyConnection(jd *util.JobDispatcher, bufPool *util.FixedBufferPool, in net.Conn,
	out *ProxyConnData, closeChan chan struct{}) {
	glog.Debugf("creating proxy for : %v <-> %v <-> %v <-> %v.\n", in.RemoteAddr(),
		in.LocalAddr(), out.conn.LocalAddr(), out.conn.RemoteAddr())
	var should_close_remote int32
	atomic.StoreInt32(&should_close_remote, 0)
	out.conn.SetReadDeadline(time.Time{})
	var wg sync.WaitGroup
	wg.Add(1)
	sendFunc := func() {
		defer wg.Done()
		if copyBytesToRemote(in, out.conn) {
			atomic.StoreInt32(&should_close_remote, 1)
		}
	}
	go sendFunc()

	if copyRspFromRemoteBuf(bufPool, out.conn, in) {
		atomic.StoreInt32(&should_close_remote, 1)
	}
	wg.Wait()
	if atomic.LoadInt32(&should_close_remote) == 1 {
		// the remote read goroutine will take charge of the destroy.
		out.Close()
	}
}

func (self *LocalProxy) doAccept(proxyParam *ProxyParam, listener net.Listener) {
	defer self.wg.Done()
	path := GetUnixDomainPath(proxyParam)
	pool := self.getConnPool(proxyParam)
	closeChan := make(chan struct{})
	var wg sync.WaitGroup

	jd := util.NewJobDispatcher(64, closeChan)

	var watchLock sync.Mutex
	watchConn := make(map[net.Conn]struct{})
	for {
		if atomic.LoadInt32(&self.stopping) == 1 {
			break
		}

		inConn, err := listener.Accept()
		if err != nil {
			if strings.HasSuffix(err.Error(), errClosed) {
				glog.Infof("Accept closed: %v.\n", err)
				break
			}
			ok := true
			self.Lock()
			var listeners map[string]net.Listener
			if listeners, ok = self.localListeners[proxyParam.PID]; ok {
				_, ok = listeners[path]
			}
			self.Unlock()
			if !ok {
				glog.Errorf("Accept removed: %v.\n", err)
				break
			} else {
				glog.Infof("Accept error: %v", err)
				continue
			}
		}
		glog.Debugf("==== New connection from: %v to %v for: %v", inConn.RemoteAddr(),
			inConn.LocalAddr(), *proxyParam)
		var outConn *ProxyConnData
		isNew := false
		success := false
		for i := 0; i < 5; i++ {
			isNew, outConn, err = pool.AcquireConn()
			if err != nil {
				success = false
				if atomic.LoadInt32(&self.stopping) == 1 {
					break
				}
				time.Sleep(time.Duration(i) * time.Millisecond * 10)
			} else {
				success = true
				break
			}
		}
		if success {
			if isNew {
				if len(proxyParam.InitSendData) > 0 {
					glog.Infof("send the init data after connected: %v.", *proxyParam)
					_, err := outConn.conn.Write([]byte(proxyParam.InitSendData))
					if err != nil {
						glog.Warningf("failed to send the init data after connected.")
						pool.DestroyRemoteConn(outConn)
						inConn.Close()
						continue
					}
				}
			}
			proxyFunc := func() {
				proxyConnection(jd, pool.bufPool, inConn, outConn, closeChan)
				pool.ReleaseConn(outConn)
				glog.Debugf("proxy connection closed: %v - %v", inConn.LocalAddr(), outConn.conn.RemoteAddr())
			}

			go func() {
				wg.Add(1)
				defer wg.Done()
				watchLock.Lock()
				watchConn[outConn.conn] = struct{}{}
				watchLock.Unlock()

				proxyFunc()

				watchLock.Lock()
				delete(watchConn, outConn.conn)
				watchLock.Unlock()
			}()
		} else {
			glog.Infof("failed to proxy connection. %v \n", *proxyParam)
			inConn.Close()
		}
	}
	self.scheduler.ScheduleJob(func() {
		close(closeChan)
		watchLock.Lock()
		for o, _ := range watchConn {
			o.Close()
		}
		watchLock.Unlock()
	}, time.Second*3, false)

	wg.Wait()
	os.Remove(path)
	self.Lock()
	if pidpool, ok := self.remoteServers[proxyParam.PID]; ok {
		delete(pidpool, proxyParam.RemoteAddr)
		if len(pidpool) == 0 {
			delete(self.remoteServers, proxyParam.PID)
		}
	}
	self.Unlock()
	glog.Infof("proxy %v stopped.", *proxyParam)
}
