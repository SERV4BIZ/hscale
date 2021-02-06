package main

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/SERV4BIZ/gfp/jsons"
	"github.com/SERV4BIZ/hscale/api/hscales"
)

func GetAppDir() string {
	ex, err := os.Executable()
	if err != nil {
		panic(err)
	}
	exPath, _ := filepath.Abs(filepath.Dir(ex))
	return exPath
}

func main() {
	pathFile := fmt.Sprint(GetAppDir(), "/config.json")
	jsoConfig, _ := jsons.JSONObjectFromFile(pathFile)
	hscales.New(jsoConfig)

	//time.Sleep(time.Hour)
}
