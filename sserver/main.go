package main

import (
	"encoding/base64"
	"flag"
	"net/http"
	"strconv"
	"strings"

	"github.com/golang/glog"
	"github.com/liuzl/goutil/rest"
	"github.com/liuzl/s"
)

var (
	addr  = flag.String("addr", ":9080", "bind address")
	path  = flag.String("path", "./stack", "task stack dir")
	stack *s.Stack
)

func PushHandler(w http.ResponseWriter, r *http.Request) {
	glog.Infof("addr=%s  method=%s host=%s uri=%s",
		r.RemoteAddr, r.Method, r.Host, r.RequestURI)
	r.ParseForm()
	data := strings.TrimSpace(r.FormValue("data"))
	if data == "" {
		rest.MustEncode(w, rest.RestMessage{"error", "data is empty"})
		return
	}
	if err := stack.Push(data); err != nil {
		rest.MustEncode(w, rest.RestMessage{"error", err.Error()})
		return
	}
	rest.MustEncode(w, rest.RestMessage{"ok", nil})
}

func PopHandler(w http.ResponseWriter, r *http.Request) {
	glog.Infof("addr=%s  method=%s host=%s uri=%s",
		r.RemoteAddr, r.Method, r.Host, r.RequestURI)
	r.ParseForm()
	t := strings.TrimSpace(r.FormValue("timeout"))
	timeout, err := strconv.ParseInt(t, 10, 64)
	if err != nil {
		timeout = 300
	}
	key, value, err := stack.Pop(timeout)
	if err != nil {
		rest.MustEncode(w, rest.RestMessage{"error", err.Error()})
		return
	}
	rest.MustEncode(w, rest.RestMessage{"ok", map[string]string{
		"key": key, "value": base64.StdEncoding.EncodeToString([]byte(value)),
	}})
}

func ConfirmHandler(w http.ResponseWriter, r *http.Request) {
	glog.Infof("addr=%s  method=%s host=%s uri=%s",
		r.RemoteAddr, r.Method, r.Host, r.RequestURI)
	r.ParseForm()
	key := strings.TrimSpace(r.FormValue("key"))
	if key == "" {
		rest.MustEncode(w, rest.RestMessage{"error", "empty key"})
		return
	}
	if err := stack.Confirm(key); err != nil {
		rest.MustEncode(w, rest.RestMessage{"error", err.Error()})
		return
	}
	rest.MustEncode(w, rest.RestMessage{"ok", nil})
}

func StatusHandler(w http.ResponseWriter, r *http.Request) {
	glog.Infof("addr=%s  method=%s host=%s uri=%s",
		r.RemoteAddr, r.Method, r.Host, r.RequestURI)
	rest.MustEncode(w, stack.Status())
}

func main() {
	flag.Parse()
	defer glog.Flush()
	var err error
	if stack, err = s.NewStack(*path); err != nil {
		glog.Fatal(err)
	}
	defer glog.Info("server exit")
	http.Handle("/pop/", rest.WithLog(PopHandler))
	http.Handle("/push/", rest.WithLog(PushHandler))
	http.Handle("/confirm/", rest.WithLog(ConfirmHandler))
	http.Handle("/status/", rest.WithLog(StatusHandler))
	glog.Info("sserver listen on", *addr)
	glog.Error(http.ListenAndServe(*addr, nil))
}
