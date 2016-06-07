package proxy

import (
	"fmt"
	"github.com/absolute8511/localproxy/httpproxy"
	"github.com/absolute8511/proxymodule/common"
	"github.com/absolute8511/proxymodule/redisproxy"
)

type ProxyManager struct {
	servers  map[string]common.ModuleProxyServer
	confList []common.ProxyConf
}

func NewProxyManager(c []common.ProxyConf) *ProxyManager {
	return &ProxyManager{
		servers:  make(map[string]common.ModuleProxyServer),
		confList: c,
	}
}

func (self *ProxyManager) StartAll(lp *LocalProxy) error {
	for _, conf := range self.confList {
		if conf.ProxyType == "HTTP" {
			s := httpproxy.NewLocalHttpProxy(conf.LocalProxyAddr,
				conf.ModuleName, conf.ModuleConfPath, lp.graceNet, lp)
			if s == nil {
				return fmt.Errorf("failed start http proxy: %v", conf.ModuleName)
			}
			go s.Start()
			self.servers[conf.ModuleName] = s
		} else if conf.ProxyType == "REDIS" {
			s := redisproxy.NewRedisProxy(conf.LocalProxyAddr,
				conf.ModuleName,
				conf.ModuleConfPath, lp.graceNet)
			if s == nil {
				return fmt.Errorf("failed start proxy: %v", conf.ModuleName)
			}
			go s.Start()
			self.servers[conf.ModuleName] = s
		} else {
			return fmt.Errorf("unknown proxy type: %v", conf.ProxyType)
		}
	}

	return nil
}

func (self *ProxyManager) StopAll() {
	for _, s := range self.servers {
		s.Stop()
	}
}
