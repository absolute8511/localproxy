package main

import (
	"fmt"
	"io/ioutil"
	"net/http"
)

func main() {
	err := http.ListenAndServe("127.0.0.1:18005", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := ioutil.ReadAll(r.Body)
		w.Write(b)
	}))
	if err != nil {
		fmt.Printf("err: %v\n", err.Error())
	}
}
