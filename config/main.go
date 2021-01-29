package main

import (
	"fmt"
	"net/http"
	"net/http/pprof"
	"runtime"
	"time"

	"github.com/SERV4BIZ/gfp/handler"

	"github.com/SERV4BIZ/hscale/config/global"
	"github.com/SERV4BIZ/hscale/config/locals"
	"github.com/SERV4BIZ/hscale/config/systems/dbprepare"
	"github.com/SERV4BIZ/hscale/config/utilities"
)

func main() {
	jsoConfig, errConfig := locals.LoadConfig()
	handler.Panic(errConfig)
	global.JSOConfig = jsoConfig

	fmt.Println("* * * * * * * * * * * * * * * * * * * * * * * * * * * * * *")
	fmt.Println(fmt.Sprint(global.AppName, " Version ", global.AppVersion))
	fmt.Println("Copyright © 2020 ", global.CompanyName, " All Rights Reserved.")
	fmt.Println("* * * * * * * * * * * * * * * * * * * * * * * * * * * * * *")
	fmt.Println(fmt.Sprint("Directory : ", utilities.GetAppDir()))
	fmt.Println("* * * * * * * * * * * * * * * * * * * * * * * * * * * * * *")
	fmt.Println("Loading configuration file.")
	fmt.Println("* * * * * * * * * * * * * * * * * * * * * * * * * * * * * *")
	fmt.Println("")
	fmt.Println(global.JSOConfig.ToString())
	fmt.Println("")
	fmt.Println("* * * * * * * * * * * * * * * * * * * * * * * * * * * * * *")
	fmt.Println("Database preparing.")
	fmt.Println("* * * * * * * * * * * * * * * * * * * * * * * * * * * * * *")
	dbprepare.Run()
	fmt.Println("* * * * * * * * * * * * * * * * * * * * * * * * * * * * * *")

	go func() {
		for {
			var m runtime.MemStats
			runtime.ReadMemStats(&m)

			global.MutexState.Lock()
			global.MemoryState = int(utilities.NumberByteToMb(m.Sys))
			global.LoadState = global.CountState
			global.CountState = 0
			global.MutexState.Unlock()

			<-time.After(time.Second)
		}
	}()

	intTime := global.JSOConfig.GetInt("int_timeout")
	if intTime <= 0 {
		intTime = 15
	}

	global.Username = global.JSOConfig.GetString("txt_username")
	global.Password = global.JSOConfig.GetString("txt_password")

	router := http.NewServeMux()
	router.HandleFunc("/", WorkHandler)

	// pprof handler
	router.HandleFunc("/debug/pprof/", pprof.Index)
	router.HandleFunc("/debug/pprof/profile", pprof.Profile)
	router.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	router.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	router.HandleFunc("/debug/pprof/trace", pprof.Trace)

	appsrv := &http.Server{
		Addr:         fmt.Sprint(":", global.JSOConfig.GetInt("int_port")),
		Handler:      router,
		ReadTimeout:  time.Duration(intTime) * time.Second,
		WriteTimeout: time.Duration(intTime) * time.Second,
	}
	appsrv.ListenAndServe()
}
