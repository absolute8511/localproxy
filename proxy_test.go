package proxy

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	. "github.com/absolute8511/proxymodule/util"
	"io/ioutil"
	"net"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

func startTestHTTPServer(handler http.Handler) (*httptest.Server, string) {
	server := httptest.NewServer(handler)
	addr := server.Listener.Addr().String()
	return server, addr
}

func TestTCPPool(t *testing.T) {
	tcppool := NewTCPConnPool(false, "testPID", "127.0.0.1:8877", NewFixedBufferPool(10, 1, true))
	isNew, conn, err := tcppool.AcquireConn()
	if err == nil {
		t.Error("expected error, but found success.")
	}
	tcppool.CleanPool()
	server, addr := startTestHTTPServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(r.URL.Path))
	}))
	defer server.Close()
	tcppool = NewTCPConnPool(false, "testPID", addr, NewFixedBufferPool(10, 1, true))
	isNew, conn, err = tcppool.AcquireConn()
	if err != nil {
		t.Error("failed acquire test server")
	} else {
		if !isNew {
			t.Error("returned connection should be new")
		}
		if tcppool.IsUnusedSince(time.Now()) {
			t.Error("The pool should not be unused.")
		}
	}
	isNew, conn2, err := tcppool.AcquireConn()
	if err != nil {
		t.Error("failed acquire test server")
		return
	}
	if !isNew {
		t.Error("returned connection should be new")
	}
	tcppool.ReleaseConn(conn)
	isNew, conn3, err := tcppool.AcquireConn()
	if err != nil {
		t.Error("failed acquire test server after release")
		return
	}
	if isNew {
		t.Error("returned connection should not be new since a conn is released")
	}
	tcppool.ReleaseConn(conn3)
	// test close and re-AcquireConn before the others release.
	// In order to avoid the same connection reused by another before the
	// previous release, we should not destroy until the acquire
	// is released.
	tcppool.DestroyRemoteConn(conn2)
	if tcppool.IsEmpty() {
		t.Error("all released, the pool should not be empty")
	}
	isNew, conn3, err = tcppool.AcquireConn()
	isNew, conn4, err := tcppool.AcquireConn()
	if err != nil {
		t.Error("failed acquire connection")
		return
	}
	tcppool.DestroyRemoteConn(conn3)
	tcppool.DestroyRemoteConn(conn4)
	if !tcppool.IsEmpty() {
		t.Error("all destroyed, the pool should be empty")
	}

	if tcppool.IsUnusedSince(time.Now().Add(-1 * time.Second)) {
		t.Error("The pool should not be unused.")
	}
	if !tcppool.IsUnusedSince(time.Now()) {
		t.Error("The pool should be unused since now.")
	}

	tcppool.CleanPool()
}

func fakeDial(proto, addr string) (net.Conn, error) {
	unixpath := "/tmp/" + strings.TrimSuffix(addr, ":80")
	//glog.Infof("dial to %v, %v\n", addr, unixpath)
	return net.DialTimeout("unix", unixpath, 1*time.Second)
}

func getProxyServerSockName(pid, remote, protocol string) (string, error) {
	var param ProxyParam
	param.PID = pid
	param.Protocol = protocol
	param.RemoteAddr = remote
	tmpbuf, _ := json.Marshal(param)

	req, err := http.NewRequest("POST", "http://localhost:8777/api/get-proxy-conn", bytes.NewBuffer(tmpbuf))
	if err != nil {
		return "", err
	}
	rsp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}

	path, err := ioutil.ReadAll(rsp.Body)
	sockname := strings.TrimPrefix(string(path), "/tmp/")
	glog.Infof("response is %v, proxy sock is %v\n", string(path), sockname)
	rsp.Body.Close()
	return sockname, nil
}

func sendRequestWithTest(c *http.Client, sockname string, path string, tmpbuf []byte) error {
	req, _ := http.NewRequest("POST", "http://"+sockname+path, bytes.NewBuffer(tmpbuf))
	//glog.Infof("== begin request: " + sockname + path)
	rsp, err := c.Do(req)
	if err != nil {
		return err
	}
	returl, err := ioutil.ReadAll(rsp.Body)
	rsp.Body.Close()
	if string(returl) != path {
		return errors.New("remote proxy response not match")
	}
	return nil
}

func sendRequestWithHTTPTest(address string, path string, tmpbuf []byte) error {
	req, _ := http.NewRequest("POST", "http://"+address+path, bytes.NewBuffer(tmpbuf))
	rsp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	returl, err := ioutil.ReadAll(rsp.Body)
	rsp.Body.Close()
	if string(returl) != path {
		return errors.New("remote proxy response not match")
	}
	return nil
}

func sendRequestWithTest2(c *http.Client, req *http.Request, path string) error {
	rsp, err := c.Do(req)
	if err != nil {
		return err
	}
	returl, err := ioutil.ReadAll(rsp.Body)
	rsp.Body.Close()
	if string(returl) != path {
		return errors.New("remote proxy response not match")
	}
	return nil
}

func TestInitSendData(t *testing.T) {
	// TODO: test the init data send logic
}

func TestLocalProxy(t *testing.T) {
	flag.Set("alsologtostderr", "true")
	//flag.Set("v", "2")
	proxy := NewLocalProxy()
	server, addr := startTestHTTPServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		buf, _ := ioutil.ReadAll(r.Body)
		glog.Infof("test server received: %v\n.", string(buf))
		if strings.Contains(r.URL.Path, "timeout") {
			time.Sleep(5 * time.Second)
		}
		if strings.Contains(r.URL.Path, "close") {
			w.Header().Set("Connection", "close")
		}
		w.Write([]byte(r.URL.Path))
	}))
	defer server.Close()

	go func() {
		err := proxy.StartLocalProxy(8777)
		_ = err
		//if err != nil {
		//	t.Fatalf("failed start local proxy: %v\n", err)
		//}
	}()
	defer proxy.Stop()

	time.Sleep(1 * time.Second)
	var param ProxyParam
	param.PID = "testPID"
	param.Protocol = "UNIX"
	param.RemoteAddr = addr
	tmpbuf, _ := json.Marshal(param)

	tr := &http.Transport{
		Dial: fakeDial,
	}
	c := &http.Client{
		Transport: tr,
	}
	sockname, err := getProxyServerSockName(param.PID, param.RemoteAddr, "unix")
	if err != nil {
		t.Errorf("get proxy failed: %v\n", err)
		return
	}

	tr.DisableKeepAlives = true
	err = sendRequestWithTest(c, string(sockname), "/echo", tmpbuf)
	if err != nil {
		t.Errorf("send by proxy failed: %v", err)
		return
	}

	// test the local connection closed and reuse
	time.Sleep(5 * time.Second)
	tr.CloseIdleConnections()

	tr.DisableKeepAlives = false
	time.Sleep(1 * time.Second)
	err = sendRequestWithTest(c, string(sockname), "/echo", tmpbuf)
	if err != nil {
		t.Errorf("send by proxy failed: %v", err)
		return
	}

	// test on the multi request on the same client connection.
	time.Sleep(1 * time.Second)
	err = sendRequestWithTest(c, string(sockname), "/echo", tmpbuf)
	if err != nil {
		t.Errorf("send by proxy failed: %v", err)
		return
	}

	// test on the remote connection close.
	time.Sleep(1 * time.Second)

	err = sendRequestWithTest(c, string(sockname), "/echo-close", tmpbuf)
	if err != nil {
		t.Errorf("send by proxy failed: %v", err)
		return
	}

	// test on the timeout on the remote.
	time.Sleep(1 * time.Second)
	c.Timeout = 1 * time.Second
	req, err := http.NewRequest("POST", "http://"+string(sockname)+"/echo-timeout", bytes.NewBuffer(tmpbuf))
	rsp, err := c.Do(req)
	if err != nil {
		glog.Infof("test timeout request : %v", err)
		req, _ = http.NewRequest("POST", "http://localhost:8777/api/close-proxy-conn", bytes.NewBuffer(tmpbuf))
		_, err = http.DefaultClient.Do(req)
		if err != nil {
			t.Errorf("request to close remote proxy failed: %v", err)
		}
	} else {
		t.Errorf("test timeout should return timeout error \n")
		rsp.Body.Close()
	}
	time.Sleep(1 * time.Second)
	err = sendRequestWithTest(c, string(sockname), "/echo", tmpbuf)
	if err != nil {
		t.Errorf("send by proxy failed: %v", err)
		return
	}

	time.Sleep(1 * time.Second)
	// http proxy test
	httpaddr, err := getProxyServerSockName(param.PID+"HTTP", param.RemoteAddr, "HTTP")
	if err != nil {
		t.Errorf("get http proxy failed: %v\n", err)
		return
	}
	httpaddr2, err := getProxyServerSockName(param.PID+"HTTP", param.RemoteAddr, "HTTP")
	if httpaddr2 != httpaddr {
		t.Errorf("get http proxy should be the same.")
		return
	}
	err = sendRequestWithHTTPTest(string(httpaddr), "/echo", tmpbuf)
	if err != nil {
		t.Errorf("send by http proxy failed: %v", err)
		return
	}
	glog.Flush()
}

func closeConnTestFunc(localclose, remoteclose bool) error {
	runtime.GOMAXPROCS(8)
	proxy := NewLocalProxy()
	server1, addr1 := startTestHTTPServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "timeout") {
			time.Sleep(5 * time.Second)
		}
		if strings.Contains(r.URL.Path, "close") {
			w.Header().Set("Connection", "close")
		}

		w.Write([]byte(r.URL.Path))
	}))
	defer server1.Close()
	go func() {
		err := proxy.StartLocalProxy(8777)
		_ = err
	}()

	var param ProxyParam
	param.PID = "testPID"
	param.Protocol = "UNIX"
	param.RemoteAddr = addr1

	defer proxy.Stop()
	tr := &http.Transport{
		Dial: fakeDial,
	}
	c := &http.Client{
		Transport: tr,
	}

	if localclose {
		tr.DisableKeepAlives = true
	}

	sockname, err := getProxyServerSockName(param.PID, param.RemoteAddr, "unix")
	if err != nil {
		glog.Errorf("request to local proxy failed: %v\n", err)
		return err
	}
	for count := 0; count < 200; count++ {
		err = sendRequestWithTest(c, string(sockname), "/echo", []byte("test"))
		if err != nil {
			glog.Errorf("send by proxy failed: %v", err)
			return err
		}
		testpath := "/echo"
		if remoteclose {
			testpath = "/echo-close"
		}
		err = sendRequestWithTest(c, string(sockname), testpath, []byte("test"))
		if err != nil {
			glog.Errorf("send by proxy failed: %v", err)
			return err
		}
	}
	mybuf, _ := json.Marshal(param)

	req, _ := http.NewRequest("POST", "http://localhost:8777/api/destroy-all", bytes.NewBuffer(mybuf))
	_, err = http.DefaultClient.Do(req)
	if err != nil {
		glog.Errorf("request to close remote proxy failed: %v", err)
		return err
	}
	glog.Flush()
	return nil
}

func TestLocalClose(t *testing.T) {
	err := closeConnTestFunc(true, false)
	if err != nil {
		t.Error(err)
	}
}

func TestRemoteClose(t *testing.T) {
	err := closeConnTestFunc(false, true)
	if err != nil {
		t.Error(err)
	}
}

func TestHTTPProxy(t *testing.T) {
}

func runTestProxyFunc(t *testing.T, pid, addr string) error {
	tr := &http.Transport{
		Dial: fakeDial,
	}
	c := &http.Client{
		Transport: tr,
	}

	tr.DisableKeepAlives = true

	sockname, err := getProxyServerSockName(pid, addr, "unix")
	if err != nil {
		glog.Errorf("request to local proxy failed: %v\n", err)
		return err
	}
	for count := 0; count < 10; count++ {
		err = sendRequestWithTest(c, string(sockname), "/echo", []byte("test"))
		if err != nil {
			glog.Errorf("======== send by proxy failed: %v", err)
			return err
		}
		err = sendRequestWithTest(c, string(sockname), "/echo-close", []byte("test"))
		if err != nil {
			glog.Errorf("send by proxy failed: %v", err)
			return err
		}
	}
	return nil
}

func destroyProxy(pid string, addr string) error {
	var tmpparam ProxyParam
	tmpparam.PID = pid
	tmpparam.RemoteAddr = addr
	mybuf, _ := json.Marshal(tmpparam)

	req, _ := http.NewRequest("POST", "http://localhost:8777/api/destroy-proxy", bytes.NewBuffer(mybuf))
	_, err := http.DefaultClient.Do(req)
	if err != nil {
		glog.Errorf("request to close remote proxy failed: %v", err)
	}
	return err
}

func TestConcurrentLocalProxy(t *testing.T) {
	runtime.GOMAXPROCS(8)
	proxy := NewLocalProxy()
	server1, addr1 := startTestHTTPServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "timeout") {
			time.Sleep(5 * time.Second)
		}
		if strings.Contains(r.URL.Path, "close") {
			w.Header().Set("Connection", "close")
		}
		w.Write([]byte(r.URL.Path))
	}))
	defer server1.Close()
	server2, addr2 := startTestHTTPServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "timeout") {
			time.Sleep(5 * time.Second)
		}
		if strings.Contains(r.URL.Path, "close") {
			w.Header().Set("Connection", "close")
		}
		w.Write([]byte(r.URL.Path))
	}))
	defer server2.Close()

	go func() {
		err := proxy.StartLocalProxy(8777)
		_ = err
		//if err != nil {
		//	t.Fatalf("failed start local proxy: %v\n", err)
		//}
	}()

	time.Sleep(2 * time.Second)
	var param ProxyParam
	param.PID = "testPID"
	param.Protocol = "UNIX"
	param.RemoteAddr = addr1

	defer proxy.Stop()
	var wg sync.WaitGroup
	wg.Add(20)
	// test many connection to the same remote
	for i := 0; i < 20; i++ {
		go func(id int) {
			err := runTestProxyFunc(t, param.PID, addr1)
			if err != nil {
				glog.Errorf("!!!!! run test error: %v", err)
				if strings.HasSuffix(err.Error(), "EOF") {
				} else {
					t.Errorf("run test error: %v", err)
				}
			}
			wg.Done()
		}(i)
	}
	wg.Add(20)
	// test many pids to many remote
	for i := 0; i < 10; i++ {
		go func(id int) {
			idstr := strconv.Itoa(id)
			err := runTestProxyFunc(t, idstr, addr1)
			if err != nil {
				glog.Errorf("!!!!! run test error: %v", err)
				if strings.HasSuffix(err.Error(), "EOF") {
				} else {
					t.Errorf("run test error: %v", err)
				}
			}
			destroyProxy(idstr, addr1)
			wg.Done()
		}(i)
		go func(id int) {
			idstr := strconv.Itoa(id)
			err := runTestProxyFunc(t, idstr, addr2)
			if err != nil {
				glog.Errorf("!!!!! run test error: %v", err)
				if strings.HasSuffix(err.Error(), "EOF") {
				} else {
					t.Errorf("run test error: %v", err)
				}
			}
			destroyProxy(idstr, addr2)
			wg.Done()
		}(i)
	}
	wg.Wait()
	destroyProxy(param.PID, addr1)
}

func TestGracefulRestart(t *testing.T) {
	//TODO:
}

func BenchmarkProxy(b *testing.B) {
	flag.Set("alsologtostderr", "true")
	proxy := NewLocalProxy()
	server1, addr1 := startTestHTTPServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Connection", "keep-alive")
		w.Write([]byte(r.URL.Path))
	}))
	defer server1.Close()

	glog.Infof("=======test remote %v======", addr1)
	//time.Sleep(time.Minute)
	go func() {
		err := proxy.StartLocalProxy(8777)
		_ = err
	}()

	time.Sleep(2 * time.Second)
	var param ProxyParam
	param.PID = "testPID"
	param.Protocol = "UNIX"
	param.RemoteAddr = addr1
	sockname, err := getProxyServerSockName(param.PID, param.RemoteAddr, "unix")
	if err != nil {
		glog.Errorf("request to local proxy failed: %v\n", err)
		return
	}

	defer proxy.Stop()
	var wg sync.WaitGroup
	wg.Add(20)
	b.ResetTimer()
	start := time.Now()
	glog.Infof("bench begin %v", time.Now())
	// test many connection to the same remote
	for i := 0; i < 20; i++ {
		go func() {
			defer wg.Done()
			tr := &http.Transport{
				Dial: fakeDial,
			}
			c := &http.Client{
				Transport: tr,
			}
			tr.DisableKeepAlives = true
			req, _ := http.NewRequest("POST", "http://"+string(sockname)+"/echo", nil)
			for count := 0; count < b.N; count++ {
				err = sendRequestWithTest2(c, req, "/echo")
				if err != nil {
					glog.Errorf("send by proxy failed: %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()
	glog.Infof("bench total: %v, total: %v", b.N, time.Now().Sub(start).Seconds())

	wg.Add(20)
	b.ResetTimer()
	start = time.Now()
	glog.Infof("bench begin %v", time.Now())
	for i := 0; i < 20; i++ {
		go func() {
			defer wg.Done()
			tr := &http.Transport{}
			c := &http.Client{
				Transport: tr,
			}
			tr.DisableKeepAlives = true
			req, _ := http.NewRequest("POST", "http://"+addr1+"/echo", nil)
			for count := 0; count < b.N; count++ {
				err = sendRequestWithTest2(c, req, "/echo")
				if err != nil {
					glog.Errorf("send by proxy failed: %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()
	glog.Infof("bench total: %v, total: %v", b.N, time.Now().Sub(start).Seconds())

}
