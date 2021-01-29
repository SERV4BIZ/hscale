package trans

import (
	"errors"
	"fmt"

	"github.com/SERV4BIZ/gfp/jsons"
	"github.com/SERV4BIZ/hscale/api/drivers/rawcmds"
	"github.com/SERV4BIZ/hscale/api/hscales/hdbs"
	"github.com/SERV4BIZ/hscale/api/utilities"
)

// GetRow is get data in database from node and shard id and keyname
func (me *HDBTx) GetRow(txtTable string, arrColumns []string, txtKeyname string) (*jsons.JSONObject, error) {
	// Check in memory
	me.HDB.MutexMapDataTable.RLock()
	dbTable, tableOk := me.HDB.MapDataTable[txtTable]
	me.HDB.MutexMapDataTable.RUnlock()

	if !tableOk {
		return nil, errors.New("Table not found")
	}

	dbTable.RLock()
	dbDataItem, itemOk := dbTable.MapDataItem[txtKeyname]
	dbTable.RUnlock()

	columns := arrColumns

	if itemOk {
		dataNodeItem := dbDataItem.DataNode

		hdbs.Reconnect(dataNodeItem)
		dataNodeItem.RLock()
		sqlSelect := dataNodeItem.JSOSQLDriver.GetString("select")
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
			return nil, errors.New("Columns is empty")
		}

		jsoResult, errRaw := rawcmds.GetRow(txConn, sqlSelect, txtTable, columns, txtKeyname)
		return jsoResult, errRaw
	}

	// If pointer not found
	me.HDB.MutexMapDataNode.RLock()
	jsaNodeKey := jsons.JSONArrayFactory()
	for key := range me.HDB.MapDataNode {
		jsaNodeKey.PutString(key)
	}
	me.HDB.MutexMapDataNode.RUnlock()

	for jsaNodeKey.Length() > 0 {
		index := utilities.RandomIntn(jsaNodeKey.Length())
		nodeName := jsaNodeKey.GetString(index)
		jsaNodeKey.Remove(index)

		me.HDB.MutexMapDataNode.RLock()
		dataNodeItem := me.HDB.MapDataNode[nodeName]
		me.HDB.MutexMapDataNode.RUnlock()

		hdbs.Reconnect(dataNodeItem)
		dataNodeItem.RLock()
		sqlSelect := dataNodeItem.JSOSQLDriver.GetString("select")
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

		jsoResult, errRaw := rawcmds.GetRow(txConn, sqlSelect, txtTable, columns, txtKeyname)
		if errRaw == nil {
			if jsoResult != nil {
				dbTable.Lock()
				dbDataItem = new(hdbs.DataItem)
				dbDataItem.DataNode = dataNodeItem
				dbDataItem.DataTable = dbTable
				dbDataItem.Name = txtKeyname
				dbTable.MapDataItem[txtKeyname] = dbDataItem
				dbTable.Unlock()
				return jsoResult, errRaw
			}
		}
	}
	return nil, errors.New("Row not found")
}
