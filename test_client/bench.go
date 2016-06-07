package main

import (
	"bytes"
	"flag"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

var (
	gnum       = flag.Uint("c", 10, "concurrent num")
	count      = flag.Uint("n", 10, "request numbers for each")
	bodyfile   = flag.String("body", "", "post body file path")
	bodyRandom = flag.Bool("bodyRandom", false, "some data will be random")
	url        = flag.String("url", "", "request dest path")
	keepAlive  = flag.Bool("keep", true, "keep alive ")
	path       = flag.String("path", "", "used for domain socket.")
	checkBody  = flag.Bool("checkbody", false, "check if the response body is equal to request body")
	headers    = flag.String("header", "", "add request header key:value, key:value")
	method     = flag.String("method", "POST", "POST/GET/PUT")
)

func fakeDial(proto, addr string) (net.Conn, error) {
	path := strings.TrimPrefix(*url, "unix://")
	return net.DialTimeout("unix", path, time.Second)
}

func main() {
	flag.Parse()
	if *url == "" {
		fmt.Printf("url is needed.\n")
		return
	}
	errNum := 0
	var body []byte
	var err error
	if *bodyfile != "" {
		body, err = ioutil.ReadFile(*bodyfile)
		if err != nil {
			fmt.Printf("read body file error:%v\n", err.Error())
			return
		}
	}
	var wg sync.WaitGroup
	s := time.Now()
	for g := 0; g < int(*gnum); g++ {
		wg.Add(1)
		go func(gindex int) {
			defer wg.Done()
			var tr *http.Transport
			requrl := ""
			if strings.HasPrefix(*url, "unix") {
				tr = &http.Transport{
					Dial: fakeDial,
				}
				requrl = "http://127.0.0.1:80" + *path
			} else {
				tr = &http.Transport{}
				requrl = *url
			}
			if !*keepAlive {
				tr.DisableKeepAlives = true
			}
			c := &http.Client{
				Transport: tr,
			}
			r := rand.New(rand.NewSource(time.Now().UnixNano()))
			gidstr := strconv.Itoa(gindex)
			for i := 0; i < int(*count); i++ {
				var req *http.Request
				if *bodyRandom {
					randstr := strconv.Itoa(int(r.Intn(10000)))
					newbody := "disk_free" + gidstr + ",hostname=server" + randstr + " value=" + randstr
					req, _ = http.NewRequest(*method, requrl, bytes.NewBuffer([]byte(newbody)))
				} else {
					req, _ = http.NewRequest(*method, requrl, bytes.NewBuffer(body))
				}
				if *headers != "" {
					headerList := strings.Split(*headers, ", ")
					for _, h := range headerList {
						vars := strings.SplitN(h, ":", 2)
						if len(vars) != 2 {
							fmt.Printf("error header: %v\n", h)
							continue
						}
						req.Header.Set(vars[0], vars[1])
						if vars[0] == "Host" {
							req.Host = vars[1]
						}
						if i < 2 && gindex < 2 {
							fmt.Printf("adding header: %v\n", vars)
							fmt.Printf("header: %v\n", req.Header)
						}
					}
				}
				rsp, err := c.Do(req)
				if err != nil {
					errNum++
					if i < 2 {
						fmt.Printf("request err: %v\n", err.Error())
					}
					if errNum > i/2 {
						fmt.Printf("request err: %v\n", err.Error())
						return
					}
					continue
				}
				if rsp.StatusCode < 200 || rsp.StatusCode > 299 {
					errNum++
					if errNum < 2 {
						fmt.Printf("response err: %v\n", rsp)
					}
				}
				ret, err := ioutil.ReadAll(rsp.Body)
				rsp.Body.Close()
				if *checkBody && i < 10 && !bytes.Equal(ret, body) {
					errNum++
					if errNum < 2 {
						fmt.Printf("return body no match : %v\n", ret)
					}
				}
			}
		}(g)
	}
	wg.Wait()
	e := time.Now()
	diff := e.Sub(s)
	qps := float64(*gnum*(*count)) / float64(diff.Seconds())
	fmt.Printf("done, errNum:%v, usedTime: %v, QPS:%v\n", errNum, diff, qps)
}
