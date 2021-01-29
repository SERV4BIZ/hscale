package main

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/SERV4BIZ/gfp/jsons"
	"github.com/SERV4BIZ/hscale/api/hscales/hdbs"
	"github.com/SERV4BIZ/hscale/api/hscales/trans"
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
	hdbItem, _ := hdbs.Factory(jsoConfig)
	b := hdbItem.DeleteRow("users", "OHM")
	fmt.Println(b)

	d := hdbItem.ExistRow("users", "OHM")
	fmt.Println(d)

	item, _ := hdbItem.GetRow("users", []string{}, "item10")
	fmt.Println(item.ToString())

	for {
		jsoScan := hdbItem.ScanRow("users", []string{"txt_keyname"}, "123")
		fmt.Println(jsoScan.ToString())

		if jsoScan.GetBool("bln_finish") {
			fmt.Println("Finished")
			break
		}
	}

	number, err := hdbItem.Counter("OHM1")
	fmt.Println(err)
	fmt.Println(number)

	fmt.Println(hdbItem.PutList("users", "item10", "lst_teams", "TEAM2"))
	//fmt.Println(hdbItem.DeleteList("users", "item10", "lst_teams", "TEAM2"))

	jsaList, _ := hdbItem.GetList("users", "item10", "lst_teams", []string{"txt_keyname"}, 0, 0)
	for i := 0; i < jsaList.Length(); i++ {
		fmt.Println(jsaList.GetObject(i).ToString())
	}

	// ScanList
	for {
		jsoScan := hdbItem.ScanList("users", "item10", "lst_teams", []string{"txt_keyname"}, "321")
		fmt.Println(jsoScan.ToString())

		if jsoScan.GetBool("bln_finish") {
			fmt.Println("Finished")
			break
		}
	}

	d = hdbItem.ExistRow("users", "OHM")
	fmt.Println(d)

	hdbTx, errTx := trans.Begin(hdbItem)
	if errTx != nil {
		fmt.Println(errTx)
	}

	jsoData := jsons.JSONObjectFactory()
	jsoData.PutString("txt_username", "watch99")
	fmt.Println(hdbTx.PutRow("users", "item1", jsoData))

	jsoData = jsons.JSONObjectFactory()
	jsoData.PutString("txt_username", "watch99")
	fmt.Println(hdbTx.PutRow("users", "item2", jsoData))
	fmt.Println(hdbTx.PutRow("users", "item3", jsoData))

	d = hdbItem.ExistRow("users", "item3")
	fmt.Println(d)

	fmt.Println(hdbTx.ExistRow("users", "item3"))

	time.Sleep(time.Hour)
}
