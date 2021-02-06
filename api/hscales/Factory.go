package hscales

import (
	"fmt"
	"strings"

	"github.com/SERV4BIZ/escondb"
	"github.com/SERV4BIZ/gfp/jsons"
	"github.com/SERV4BIZ/gfp/uuid"
)

// Factory is begin HScaleDB object
func Factory(jsoConfigHost *jsons.JSONObject) (*HDB, error) {
	myUUID, errUUID := uuid.NewV4()
	if errUUID != nil {
		return nil, errUUID
	}

	hdbItem := new(HDB)
	hdbItem.UUID = myUUID
	hdbItem.JSOConfigHost = jsoConfigHost
	hdbItem.MapDataNode = make(map[string]*DataNode)
	hdbItem.MapDataScan = make(map[string]*DataScan)
	fmt.Println("* * * * * * * * * * * * * * * * * * * * * * * * * * * * * *")
	fmt.Println("HScale DB Cluster Factory")
	fmt.Println("* * * * * * * * * * * * * * * * * * * * * * * * * * * * * *")
	fmt.Println(fmt.Sprint("UUID : ", hdbItem.UUID))
	fmt.Println("* * * * * * * * * * * * * * * * * * * * * * * * * * * * * *")
	fmt.Println("Loading datanode info.")
	fmt.Println("* * * * * * * * * * * * * * * * * * * * * * * * * * * * * *")

	// Get Database info
	dbName := hdbItem.JSOConfigHost.GetString("txt_dbname")
	jsoDBInfo, errDBInfo := hdbItem.DataBaseInfo(dbName)
	if errDBInfo != nil {
		return nil, errDBInfo
	}
	hdbItem.DBName = jsoDBInfo.GetString("txt_name")
	hdbItem.JSOSchema = jsoDBInfo.GetObject("jso_schema")
	hdbItem.MapDataTable = make(map[string]*DataTable)

	schemaKeys := hdbItem.JSOSchema.GetKeys()
	for _, schemaName := range schemaKeys {
		dbTable := new(DataTable)
		dbTable.HDB = hdbItem
		dbTable.Name = strings.ToLower(strings.TrimSpace(schemaName))
		dbTable.MapDataItem = make(map[string]*DataItem)
		hdbItem.MapDataTable[dbTable.Name] = dbTable
	}

	// Get Data node info
	jsaDataNode, errDataNode := hdbItem.DataNodeListing()
	if errDataNode != nil {
		return nil, errDataNode
	}

	for i := 0; i < jsaDataNode.Length(); i++ {
		buff := fmt.Sprint(i+1, " ) ", jsaDataNode.GetString(i))
		fmt.Println(buff)

		jsoNodeInfo, errNodeInfo := hdbItem.DataNodeInfo(jsaDataNode.GetString(i))
		if errNodeInfo != nil {
			return nil, errNodeInfo
		}

		nNodeItem := new(DataNode)
		nNodeItem.HDB = hdbItem
		nNodeItem.Name = strings.ToLower(strings.TrimSpace(jsoNodeInfo.GetString("txt_name")))
		nNodeItem.JSODataBase = jsoNodeInfo.GetObject("jso_database")
		nNodeItem.JSOSQLDriver = jsoNodeInfo.GetObject("jso_sqldriver")
		nNodeItem.DBConn = nil
		nNodeItem.MapDBTx = make(map[string]*escondb.ESCONTX)

		errConn := nNodeItem.Connect()
		if errConn != nil {
			// Close all connection
			hdbItem.MutexMapDataNode.RLock()
			for keyName := range hdbItem.MapDataNode {
				dbItem := hdbItem.MapDataNode[keyName].DBConn
				if dbItem != nil {
					dbItem.Close()
				}
			}
			hdbItem.MutexMapDataNode.RUnlock()
			return nil, errConn
		}

		hdbItem.MutexMapDataNode.Lock()
		hdbItem.MapDataNode[nNodeItem.Name] = nNodeItem
		hdbItem.MutexMapDataNode.Unlock()
	}
	fmt.Println("* * * * * * * * * * * * * * * * * * * * * * * * * * * * * *")

	return hdbItem, nil
}
