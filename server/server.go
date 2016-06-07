package main

import (
	"flag"
	"github.com/absolute8511/glog"
	"github.com/absolute8511/localproxy"
	"github.com/absolute8511/proxymodule/common"
	"github.com/absolute8511/proxymodule/util"
	_ "net/http/pprof"
	"os"
	"runtime"
	"runtime/pprof"
	"time"
)

var version = "No Version Provided"
var buildstamp = time.Now().UTC().String()
var compiler = runtime.Compiler + " " + runtime.Version() + " " + runtime.GOOS + " " + runtime.GOARCH

var (
	port       = flag.Uint("port", 18001, "Listen port on localhost")
	pproffile  = flag.String("pproffile", "", "write pprof to file if not empty")
	configFile = flag.String("config", "./proxy.conf", "the proxy configuration file")
)

func main() {
	flag.Parse()
	confList, err := common.LoadFromFile(*configFile)
	if err != nil {
		glog.Errorf("Load configure failed: %v", err.Error())
		return
	}

	ticker := time.NewTicker(time.Second * 1)
	go func() {
		for {
			select {
			case _, ok := <-ticker.C:
				if !ok {
					return
				}
				glog.Flush()
			}
		}
	}()
	glog.Infof("Current Version: %v, build time : %v, build compiler: %v", version, buildstamp, compiler)
	lp := proxy.NewLocalProxy()

	defer glog.Flush()

	proxyMgr := proxy.NewProxyManager(confList)
	err = proxyMgr.StartAll(lp)
	if err != nil {
		glog.Errorf("failed: %v", err.Error())
		return
	}

	quitChan := make(chan bool)
	util.Trap(func() {
		proxyMgr.StopAll()
		lp.Stop()
		if *pproffile != "" {
			pprof.StopCPUProfile()
		}
		close(quitChan)
	}, lp.Restart)
	if *pproffile != "" {
		f, err := os.Create(*pproffile)
		if err != nil {
			glog.Warningf("failed create pprof file: %v", err)
		} else {
			pprof.StartCPUProfile(f)
			defer pprof.StopCPUProfile()
		}
	}

	err = lp.StartLocalProxy(uint16(*port))
	<-quitChan
	ticker.Stop()
	glog.Infof("Proxy server exit: %v", err)
}
