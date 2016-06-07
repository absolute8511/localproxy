package client

import (
	"bytes"
	"encoding/json"
	"errors"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"strconv"
	"sync/atomic"
	"time"
)

var connId int32

func NextConnId() int32 {
	return atomic.AddInt32(&connId, 1)
}

func GetProxyAddress(port string, remote string, cid int, protocol string) (string, error) {
	var param proxy.ProxyParam
	param.ConnId = cid
	param.Protocol = protocol
	param.RemoteAddr = remote
	param.PID = strconv.Itoa(os.Getpid())
	tmp, _ := json.Marshal(param)
	req, err := http.NewRequest("POST", "http://localhost:"+port+"/api/get-proxy-conn", bytes.NewBuffer(tmp))
	if err != nil {
		return "", errors.New("Failed connect local proxy :" + err.Error())
	}

	rsp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", errors.New("Failed connect local proxy :" + err.Error())
	}
	path, err := ioutil.ReadAll(rsp.Body)

	rsp.Body.Close()
	return string(path), err
}

func ConnectByUnixProxy(localProxyPort string, remote string, cid int, timeout time.Duration) (net.Conn, error) {
	addr, err := GetProxyAddress(localProxyPort, remote, cid, "unix")
	if err != nil {
		return nil, err
	}
	return net.DialTimeout("unix", addr, timeout)
}

func ConnectByHTTPProxy(localProxyPort string, remote string, cid int, timeout time.Duration) (net.Conn, error) {
	addr, err := GetProxyAddress(localProxyPort, remote, cid, "http")
	if err != nil {
		return nil, err
	}
	return net.DialTimeout("tcp", addr, timeout)
}
