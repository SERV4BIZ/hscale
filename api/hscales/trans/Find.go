package trans

import (
	"errors"
	"fmt"

	"github.com/SERV4BIZ/gfp/jsons"
	"github.com/SERV4BIZ/hscale/api/drivers/rawcmds"
	"github.com/SERV4BIZ/hscale/api/hscales/hdbs"
	"github.com/SERV4BIZ/hscale/api/utilities"
)

// Find is get data in database from node and shard id and keyname
func (me *HDBTx) Find(txtTable string, arrColumns []string, txtConditions string, intLimit int) (*jsons.JSONArray, error) {
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
		index := utilities.RandomIntn(jsaNodeKey.Length())
		nodeName := jsaNodeKey.GetString(index)
		jsaNodeKey.Remove(index)

		me.HDB.MutexMapDataNode.RLock()
		dataNodeItem := me.HDB.MapDataNode[nodeName]
		me.HDB.MutexMapDataNode.RUnlock()

		hdbs.Reconnect(dataNodeItem)
		dataNodeItem.RLock()
		sqlFind := dataNodeItem.JSOSQLDriver.GetString("find")
		sqlFindLimit := dataNodeItem.JSOSQLDriver.GetString("find_limit")
		sqlListColumn := dataNodeItem.JSOSQLDriver.GetString("listing_column")
		dataNodeItem.RUnlock()

		if dataNodeItem.DBConn == nil {
			return nil, errors.New("Connection is not open")
		}

		dataNodeItem.MutexMapDBTx.Lock()
		txConn, txOk := dataNodeItem.MapDBTx[me.UUID]
		dataNodeItem.MutexMapDBTx.Unlock()

		if !txOk {
			return nil, errors.New("Connection transaction not found")
		}

		if txConn == nil {
			return nil, errors.New("Connection transaction not open")
		}

		var errColumns error
		if len(columns) == 0 {
			columns, errColumns = rawcmds.ListColumns(txConn, sqlListColumn, txtTable)
		}

		if errColumns != nil {
			return nil, errors.New(fmt.Sprint("Columns is empty [ ", errColumns, " ]"))
		}

		sqlQuery := sqlFind
		if intLimit >= 0 {
			sqlQuery = sqlFindLimit
		}

		jsaRaw, errRaw := rawcmds.Find(txConn, sqlQuery, txtTable, columns, txtConditions, intLimit)
		if errRaw != nil {
			return nil, errors.New(fmt.Sprint("Can not find data from raw command [ ", errRaw, " ]"))
		}

		for i := 0; i < jsaRaw.Length(); i++ {
			jsaListing.PutObject(jsaRaw.GetObject(i))
		}
	}
	return jsaListing, nil
}
