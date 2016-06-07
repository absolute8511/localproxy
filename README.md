# localproxy

Usage
======
./server -port=18001 -v=2 -log_dir="/data/logs" -alsotostderr=true

See ./server -h for the details.

HTTP API
========
/api/get-proxy-conn: get a dynamic proxy address.

POST data :
```
{
    "PID":"nsq-12",
    "RemoteAddr":"10.10.1.100:12345",
    "Protocol":"unix",
    "InitSendData":""
}
```
Note: PID is used to separate the different client to the same service,
    basically, we can use the same pid for all clients. However, we can
    separate it to avoid some contention.
    InitSendData: this can be used to init the connection after the success
    connect. (such as the auth or identify client.)

/api/close-proxy-conn: close the remote immediately.

POST data:
```
{
    "PID":"nsq-12",
    "RemoteAddr":"10.10.1.100:12345",
    "Protocol":"unix",
    "InitSendData":""
}
```
Note: This will close all the connections to the remote associated with PID.

/api/destroy-proxy: close all the remote and stop the dynamic proxy listener
associated with the specific remote address and PID.

POST data:
```
{
    "PID":"nsq-12",
    "RemoteAddr":"10.10.1.100:12345",
    "Protocol":"unix",
    "InitSendData":""
}
```
Note: If this dynamic proxy is not used anymore, the client can destroy it using
this API. Basically, this is not necessary since the proxy can handle this
situation.

/api/destroy-all: close and stop all the dynamic proxy listener
associated with the specific PID.
```
{
    "PID":"nsq-12",
}
```

Note: Destroy all the proxy used by the PID. Basically, this is not necessary since the proxy can handle this
situation.

/api/status: see the current status for the proxy connections.

/api/debug-status: see the current status for the proxy connections and print
the debug info for details.


Graceful restart/upgrade
=========================
Kill with signal SIGUSR2 will trigger a graceful restart and this can be used
to upgrade the proxy binary without affecting the services.

Configuration for the static proxy
================
```
[
{
    "ModuleName":"nsqlookup",
    "ProxyType":"HTTP",
    "LocalProxyAddr":"127.0.0.1:18003",
    "RemoteAddrList":[
        "192.168.66.202:4161"
    ]
}
]
```
Note: the static proxy configure can be used to start some proxy server
without calling the dynamic API.

NSQ lookup HTTP proxy
==========
The proxy can be configured in the static configure file.
Currently only lookup is supported on the nsq.The API is the same without proxy.
With proxy, all returned nsqd ip:port will be converted to local proxy
address. If you do need the orig, you can add query param with
`"?disableconvert=true"`, and the cache is enabled by default. By using
"?disablecache=true" you can disable the cache.

