package hdbs

import (
	"sync"

	"github.com/SERV4BIZ/escondb"
	"github.com/SERV4BIZ/gfp/jsons"
)

// DataScan is data struct for job scan data
type DataScan struct {
	sync.RWMutex
	HDBTX *HDBTX

	Stamp   float64
	Read    float64
	UUID    string
	JSAList *jsons.JSONArray

	IsError  bool
	ErrorMsg string

	IsFinish bool
	IsExpire bool
}

// DataNode is struct for datanode info
type DataNode struct {
	sync.RWMutex
	HDB *HDB

	Name         string
	JSODataBase  *jsons.JSONObject
	JSOSQLDriver *jsons.JSONObject

	DBConn *escondb.ESCONDB

	MutexMapDBTx sync.RWMutex
	MapDBTx      map[string]*escondb.ESCONTX
}

// DataTable is struct in Database object
type DataTable struct {
	sync.RWMutex
	HDB  *HDB
	Name string

	MapDataItem map[string]*DataItem
}

// DataItem is struct in Cabinet object
type DataItem struct {
	DataNode  *DataNode
	DataTable *DataTable
	Name      string
}

// HDB is main object
type HDB struct {
	sync.RWMutex

	UUID          string
	JSOConfigHost *jsons.JSONObject

	MutexMapDataNode sync.RWMutex
	MapDataNode      map[string]*DataNode

	MutexMapDataScan sync.RWMutex
	MapDataScan      map[string]*DataScan

	DBName    string
	JSOSchema *jsons.JSONObject

	MutexMapDataTable sync.RWMutex
	MapDataTable      map[string]*DataTable
}

// HDBTX is tx object
type HDBTX struct {
	sync.RWMutex
	HDB  *HDB
	UUID string
}
