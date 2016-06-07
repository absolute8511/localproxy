package httpproxy

import (
	"bytes"
	"encoding/json"
	"errors"
	"github.com/bitly/go-simplejson"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"
)

type Producer struct {
	RemoteAddress    string `json:"remote_address"`
	Hostname         string `json:"hostname"`
	BroadcastAddress string `json:"broadcast_address"`
	TCPPort          int    `json:"tcp_port"`
	HTTPPort         int    `json:"http_port"`
	Version          string `json:"version"`
}

type CacheData struct {
	orig        []byte
	proxyedData []byte
}

type NsqLookupHttpProxy struct {
	sync.RWMutex
	cachedBody     map[string]CacheData
	cachedNodes    CacheData
	localproxy     ILocalProxy
	proxyAddresses map[string]struct{}
	quitChan       chan struct{}
}

func CreateNsqLookupHttpProxy(p ILocalProxy) HttpProxyModule {
	return &NsqLookupHttpProxy{
		cachedBody:     make(map[string]CacheData, 100),
		localproxy:     p,
		proxyAddresses: make(map[string]struct{}, 10),
		quitChan:       make(chan struct{}),
	}
}

func (self *NsqLookupHttpProxy) GetProxyName() string {
	return "nsqlookup"
}

func (self *NsqLookupHttpProxy) Stop() {
	close(self.quitChan)
}

func (self *NsqLookupHttpProxy) ProxyHandler(proxyaddr string, w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path
	remoteLookupAddr := r.URL.Query().Get("lookupd")
	disableCache := r.URL.Query().Get("disablecache") == "true"
	disableConvert := r.URL.Query().Get("disableconvert") == "true"
	switch path {
	case "/lookup":
		// currently the proxyaddr is not used.
		// the remote lookupd address should be provided in the query param.
		if remoteLookupAddr == "" {
			http.Error(w, "Missing lookupd param", http.StatusBadRequest)
			return
		}
		self.HandleLookup(remoteLookupAddr, disableCache, disableConvert, w, r)
	case "/nodes":
		if remoteLookupAddr == "" {
			http.Error(w, "Missing lookupd param", http.StatusBadRequest)
			return
		}
		self.HandleNodes(remoteLookupAddr, disableCache, disableConvert, w, r)
	default:
		httpProxyLog.Infof("proxy not implemented: %v", path)
		http.NotFound(w, r)
	}
}

func (self *NsqLookupHttpProxy) HandleNodes(proxyaddr string, disableCache, disableConvert bool, w http.ResponseWriter, r *http.Request) {
	httpProxyLog.Debugf("got lookup proxy %v to remote %v", r.URL.String(), proxyaddr)
	self.RLock()
	if !disableCache {
		cached := self.cachedNodes
		if cached.proxyedData != nil {
			if disableConvert {
				w.Write(cached.orig)
			} else {
				w.Write(cached.proxyedData)
			}
			self.RUnlock()
			return
		}
	}
	self.RUnlock()
	origData, proxyedBody, err := self.getLookupdNodesData(proxyaddr)
	if err != nil {
		httpProxyLog.Infof("get nodes failed: %v", err.Error())
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	self.Lock()
	if _, ok := self.proxyAddresses[proxyaddr+"-nodes"]; !ok {
		go self.queryNsqdNodesLoop(proxyaddr)
		self.proxyAddresses[proxyaddr+"-nodes"] = struct{}{}
	}

	self.cachedNodes = CacheData{orig: origData, proxyedData: proxyedBody}
	self.Unlock()
	if disableConvert {
		w.Write(origData)
	} else {
		w.Write(proxyedBody)
	}
}

func (self *NsqLookupHttpProxy) HandleLookup(proxyaddr string, disableCache, disableConvert bool, w http.ResponseWriter, r *http.Request) {
	topicName := r.URL.Query().Get("topic")
	if topicName == "" {
		http.Error(w, "Missing topic", http.StatusBadRequest)
		return
	}
	httpProxyLog.Debugf("got lookup proxy %v to remote %v", r.URL.String(), proxyaddr)
	self.RLock()
	if !disableCache {
		cached, ok := self.cachedBody[topicName]
		if ok && cached.proxyedData != nil {
			if disableConvert {
				w.Write(cached.orig)
			} else {
				w.Write(cached.proxyedData)
			}
			self.RUnlock()
			return
		}
	}
	self.RUnlock()
	origData, proxyedBody, err := self.getLookupdTopicData(topicName, proxyaddr)
	if err != nil {
		httpProxyLog.Infof("get topic failed: %v", err.Error())
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	self.Lock()
	if _, ok := self.proxyAddresses[proxyaddr]; !ok {
		go self.queryLookupLoop(proxyaddr)
		self.proxyAddresses[proxyaddr] = struct{}{}
	}

	self.cachedBody[topicName] = CacheData{orig: origData, proxyedData: proxyedBody}
	self.Unlock()
	if disableConvert {
		w.Write(origData)
	} else {
		w.Write(proxyedBody)
	}
}

func (self *NsqLookupHttpProxy) queryNsqdNodesLoop(proxyaddr string) {
	ticker := time.NewTicker(time.Minute)
	for {
		select {
		case <-ticker.C:
			self.RLock()
			newCached := self.cachedNodes
			self.RUnlock()
			o, c, err := self.getLookupdNodesData(proxyaddr)
			if err == nil {
				if !bytes.Equal(newCached.proxyedData, c) {
					newCached = CacheData{o, c}
				} else {
					newCached = CacheData{nil, nil}
					continue
				}
			} else {
				httpProxyLog.Infof("get nodes failed: %v", err.Error())
				continue
			}
			self.Lock()
			if newCached.proxyedData == nil {
				continue
			}
			self.cachedNodes = newCached
			httpProxyLog.Debugf("cached nodes updated")
			self.Unlock()
		case <-self.quitChan:
			return
		}
	}
}

func (self *NsqLookupHttpProxy) queryLookupLoop(proxyaddr string) {

	ticker := time.NewTicker(time.Minute)
	for {
		select {
		case <-ticker.C:
			newCached := make(map[string]CacheData)
			self.RLock()
			for t, old := range self.cachedBody {
				newCached[t] = old
			}
			self.RUnlock()
			for t, old := range newCached {
				o, c, err := self.getLookupdTopicData(t, proxyaddr)
				if err == nil {
					if !bytes.Equal(old.proxyedData, c) {
						newCached[t] = CacheData{o, c}
					} else {
						newCached[t] = CacheData{nil, nil}
						delete(newCached, t)
					}
				} else {
					httpProxyLog.Infof("get topic failed: %v", err.Error())
				}
			}
			if len(newCached) == 0 {
				continue
			}
			self.Lock()
			for t, newData := range newCached {
				if newData.proxyedData == nil {
					continue
				}
				self.cachedBody[t] = newData
				httpProxyLog.Debugf("cached topic %v updated", t)
			}
			self.Unlock()
		case <-self.quitChan:
			return
		}
	}
}

func (self *NsqLookupHttpProxy) getLookupdTopicData(t string, proxyAddr string) ([]byte, []byte, error) {
	var addr url.URL
	addr.Scheme = "http"
	addr.Host = proxyAddr
	addr.Path = "lookup"
	q := addr.Query()
	q.Set("topic", t)
	addr.RawQuery = q.Encode()
	return self.getLookupdData(&addr)
}

func (self *NsqLookupHttpProxy) getLookupdNodesData(proxyAddr string) ([]byte, []byte, error) {
	var addr url.URL
	addr.Scheme = "http"
	addr.Host = proxyAddr
	addr.Path = "nodes"
	return self.getLookupdData(&addr)
}

func (self *NsqLookupHttpProxy) getLookupdData(addr *url.URL) ([]byte, []byte, error) {
	req, err := http.NewRequest("GET", addr.String(), nil)
	if err != nil {
		return nil, nil, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, nil, err
	}
	defer resp.Body.Close()
	c, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, nil, err
	}
	if resp.StatusCode != 200 {
		return nil, nil, errors.New(resp.Status)
	}
	if len(c) == 0 {
		c = []byte("{}")
	}

	data, err := simplejson.NewJson(c)
	if err != nil {
		return nil, nil, err
	}
	var code int
	var txt string
	if resp.Header.Get("X-NSQ-Content-Type") == "nsq; version=1.0" {
	} else {
		code = data.Get("status_code").MustInt()
		txt = data.Get("status_txt").MustString()
		data = data.Get("data")
	}
	for i := range data.Get("producers").MustArray() {
		producer := data.Get("producers").GetIndex(i)
		broadcastAddress := producer.Get("broadcast_address").MustString()
		port := producer.Get("tcp_port").MustInt()
		httpport := producer.Get("http_port").MustInt()
		tcpaddr := net.JoinHostPort(broadcastAddress, strconv.Itoa(port))
		httpaddr := net.JoinHostPort(broadcastAddress, strconv.Itoa(httpport))

		proxyaddr, err := self.localproxy.GetProxy("nsqd-http", "HTTP", httpaddr, "")
		if err != nil {
			httpProxyLog.Warningf("start nsqlookup http proxy failed.")
			continue
		}
		h, p, err := net.SplitHostPort(proxyaddr)
		if err == nil {
			producer.Set("broadcast_address", h)
			producer.Set("http_port", p)
		}
		proxyaddr, err = self.localproxy.GetProxy("nsqd-tcp", "HTTP", tcpaddr, "  V2")
		if err != nil {
			httpProxyLog.Warningf("start nsqlookup tcp proxy failed.")
			continue
		}
		h, p, err = net.SplitHostPort(proxyaddr)
		if err == nil {
			producer.Set("tcp_port", p)
		}
	}

	orig := c
	if resp.Header.Get("X-NSQ-Content-Type") == "nsq; version=1.0" {
		c, err = data.MarshalJSON()
	} else {
		c, err = json.Marshal(struct {
			StatusCode int         `json:"status_code"`
			StatusTxt  string      `json:"status_txt"`
			Data       interface{} `json:"data"`
		}{
			code,
			txt,
			data,
		})
	}
	httpProxyLog.Debugf("orig %v after proxy, response is : %v", string(orig), string(c))
	return orig, c, nil
}
