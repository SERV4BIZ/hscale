package hdbs

import (
	"errors"
	"fmt"

	"github.com/SERV4BIZ/gfp/jsons"
	"github.com/SERV4BIZ/hscale/api/drivers"
	"github.com/SERV4BIZ/hscale/api/utility"
)

// GetAll is get data all in database from node
func (me *HDBTX) GetAll(txtTable string, arrColumns []string, intLimit int) (*jsons.JSONArray, error) {
	// Check in memory
	me.HDB.MutexMapDataTable.RLock()
	_, tableOk := me.HDB.MapDataTable[txtTable]
	me.HDB.MutexMapDataTable.RUnlock()

	if !tableOk {
		return nil, errors.New("Table not found")
	}
	columns := arrColumns

	me.HDB.MutexMapDataNode.RLock()
	jsaNodeKey := jsons.JSONArrayFactory()
	for key := range me.HDB.MapDataNode {
		jsaNodeKey.PutString(key)
	}
	me.HDB.MutexMapDataNode.RUnlock()

	jsaListing := jsons.JSONArrayFactory()
	for jsaNodeKey.Length() > 0 {
		index := utility.RandomIntn(jsaNodeKey.Length())
		nodeName := jsaNodeKey.GetString(index)
		jsaNodeKey.Remove(index)

		me.HDB.MutexMapDataNode.RLock()
		dataNodeItem := me.HDB.MapDataNode[nodeName]
		me.HDB.MutexMapDataNode.RUnlock()

		dataNodeItem.Reconnect()
		if dataNodeItem.DBConn == nil {
			return nil, errors.New("Connection is not open")
		}

		dataNodeItem.RLock()
		sqlSelectAll := dataNodeItem.JSOSQLDriver.GetString("select_all")
		sqlSelectAllLimit := dataNodeItem.JSOSQLDriver.GetString("select_all_limit")
		sqlListColumn := dataNodeItem.JSOSQLDriver.GetString("listing_column")
		dataNodeItem.RUnlock()

		dataNodeItem.MutexMapDBTx.Lock()
		dbTx, dbTxOk  := dataNodeItem.MapDBTx[me.UUID]
		dataNodeItem.MutexMapDBTx.Unlock()

		if !dbTxOk {
			return nil,errors.New("Database transaction not found")
		}

		var errColumns error
		if len(columns) == 0 {
			columns, errColumns = drivers.ListColumns(dbTx, sqlListColumn, txtTable)
		}

		if errColumns != nil {
			return nil, errors.New(fmt.Sprint("Columns is empty [ ", errColumns, " ]"))
		}

		sqlQuery := sqlSelectAll
		if intLimit >= 0 {
			sqlQuery = sqlSelectAllLimit
		}

		jsaRaw, errRaw := drivers.GetAll(dbTx, sqlQuery, txtTable, columns, intLimit)
		if errRaw != nil {
			return nil, errors.New(fmt.Sprint("Can not find data from raw command [ ", errRaw, " ]"))
		}

		for i := 0; i < jsaRaw.Length(); i++ {
			jsaListing.PutObject(jsaRaw.GetObject(i))
		}
	}
	return jsaListing, nil
}
