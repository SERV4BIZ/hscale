package dbprepare

import (
	"fmt"

	"github.com/SERV4BIZ/gfp/handler"
	"github.com/SERV4BIZ/hscale/config/locals"
)

// Run is preparing database to ready for all working
func Run() {
	jsaDataNode, errDataNode := locals.ListDataNode()
	handler.Panic(errDataNode)

	for i := 0; i < jsaDataNode.Length(); i++ {
		fmt.Println(fmt.Sprint((i + 1), " ) ", jsaDataNode.GetString(i)))

		jsoNodeinfo, errNodeinfo := locals.LoadDataNodeInfo(jsaDataNode.GetString(i))
		handler.Panic(errNodeinfo)

		jsoDatabase := jsoNodeinfo.GetObject("jso_database")
		driverName := jsoDatabase.GetString("txt_driver")

		conn, errConn := Connect(driverName, jsoDatabase.GetString("txt_host"), jsoDatabase.GetInt("int_port"), jsoDatabase.GetString("txt_username"), jsoDatabase.GetString("txt_password"), jsoDatabase.GetString("txt_dbname"))
		if handler.Error(errConn) {
			panic(fmt.Sprint(jsaDataNode.GetString(i), " ", driverName, " ", jsoDatabase.GetString("txt_host"), " ", jsoDatabase.GetString("txt_dbname"), " [] ", errConn, " ]"))
		}
		defer conn.Close()

		// Create Database
		CreateDatabase(jsoNodeinfo, jsoDatabase, driverName, conn)
	}
}
