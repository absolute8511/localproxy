package httpproxy

import (
	"github.com/absolute8511/grace/gracenet"
	"github.com/absolute8511/proxymodule/common"
	"net"
	"net/http"
)

var httpProxyLog = common.NewLevelLogger(1, nil)

type ILocalProxy interface {
	GetProxy(pid, protocol, remote, initData string) (string, error)
}

type HttpProxyModule interface {
	ProxyHandler(addr string, w http.ResponseWriter, r *http.Request)
	Stop()
	GetProxyName() string
}

type HttpProxyCreateFunc func(p ILocalProxy) HttpProxyModule

var gHttpProxyModuleFactory map[string]HttpProxyCreateFunc

func init() {
	gHttpProxyModuleFactory = make(map[string]HttpProxyCreateFunc)
	gHttpProxyModuleFactory["nsqlookup"] = CreateNsqLookupHttpProxy
}

func RegisterHttpProxyModule(name string, h HttpProxyCreateFunc) {
	gHttpProxyModuleFactory[name] = h
}

func UnregisterHttpProxyModule(name string) {
	delete(gHttpProxyModuleFactory, name)
}

type LocalHttpProxy struct {
	laddr           string
	grace           *gracenet.Net
	remoteAddresses []string
	proxyModule     HttpProxyModule
	l               net.Listener
}

func NewLocalHttpProxy(addr string, module string, moduleConf string, grace *gracenet.Net, p ILocalProxy) *LocalHttpProxy {
	if _, ok := gHttpProxyModuleFactory[module]; !ok {
		httpProxyLog.Warningf("http proxy module not found: %v", module)
		return nil
	}

	remotes := make([]string, 0)
	// TODO: read remote proxy from module configure file
	return &LocalHttpProxy{
		laddr:           addr,
		grace:           grace,
		remoteAddresses: remotes,
		proxyModule:     gHttpProxyModuleFactory[module](p),
	}
}

func (self *LocalHttpProxy) Start() {
	httpProxyLog.Infof("http proxy %v on : %v", self.proxyModule.GetProxyName(), self.laddr)

	var err error
	self.l, err = self.grace.Listen("tcp", self.laddr)
	if err != nil {
		httpProxyLog.Errorf("err: %v", err)
		return
	}

	http.Serve(self.l, self)
	httpProxyLog.Infof("http proxy %v stopped.", self.proxyModule.GetProxyName())
}

func (self *LocalHttpProxy) Stop() {
	self.l.Close()
	self.proxyModule.Stop()
}

func (self *LocalHttpProxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	addr := self.getProxyAddr()
	self.proxyModule.ProxyHandler(addr, w, r)
}

func (self *LocalHttpProxy) getProxyAddr() string {
	if len(self.remoteAddresses) > 0 {
		return self.remoteAddresses[0]
	}
	return ""
}
