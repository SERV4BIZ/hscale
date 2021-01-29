package hdbs

import (
	"errors"
	"fmt"

	"github.com/SERV4BIZ/gfp/jsons"
	"github.com/SERV4BIZ/hscale/api/drivers/rawcmds"
	"github.com/SERV4BIZ/hscale/api/utilities"
)

// GetRow is get data in database from node and shard id and keyname
func (me *HDB) GetRow(txtTable string, arrColumns []string, txtKeyname string) (*jsons.JSONObject, error) {
	// Check in memory
	me.MutexMapDataTable.RLock()
	dbTable, tableOk := me.MapDataTable[txtTable]
	me.MutexMapDataTable.RUnlock()

	if !tableOk {
		return nil, errors.New("Table not found")
	}

	dbTable.RLock()
	dbDataItem, itemOk := dbTable.MapDataItem[txtKeyname]
	dbTable.RUnlock()

	columns := arrColumns

	if itemOk {
		dataNodeItem := dbDataItem.DataNode

		Reconnect(dataNodeItem)
		dataNodeItem.RLock()
		sqlSelect := dataNodeItem.JSOSQLDriver.GetString("select")
		sqlListColumn := dataNodeItem.JSOSQLDriver.GetString("listing_column")
		dataNodeItem.RUnlock()

		if dataNodeItem.DBConn == nil {
			return nil, errors.New("Connection is not open")
		}

		var errColumns error
		if len(columns) == 0 {
			columns, errColumns = rawcmds.ListColumns(dataNodeItem.DBConn, sqlListColumn, txtTable)
		}

		if errColumns != nil {
			return nil, errors.New("Columns is empty")
		}

		jsoResult, errRaw := rawcmds.GetRow(dataNodeItem.DBConn, sqlSelect, txtTable, columns, txtKeyname)
		return jsoResult, errRaw
	}

	// If pointer not found
	me.MutexMapDataNode.RLock()
	jsaNodeKey := jsons.JSONArrayFactory()
	for key := range me.MapDataNode {
		jsaNodeKey.PutString(key)
	}
	me.MutexMapDataNode.RUnlock()

	for jsaNodeKey.Length() > 0 {
		index := utilities.RandomIntn(jsaNodeKey.Length())
		nodeName := jsaNodeKey.GetString(index)
		jsaNodeKey.Remove(index)

		me.MutexMapDataNode.RLock()
		dataNodeItem := me.MapDataNode[nodeName]
		me.MutexMapDataNode.RUnlock()

		Reconnect(dataNodeItem)
		dataNodeItem.RLock()
		sqlSelect := dataNodeItem.JSOSQLDriver.GetString("select")
		sqlListColumn := dataNodeItem.JSOSQLDriver.GetString("listing_column")
		dataNodeItem.RUnlock()

		if dataNodeItem.DBConn == nil {
			return nil, errors.New("Connection is not open")
		}

		var errColumns error
		if len(columns) == 0 {
			columns, errColumns = rawcmds.ListColumns(dataNodeItem.DBConn, sqlListColumn, txtTable)
		}

		if errColumns != nil {
			return nil, errors.New(fmt.Sprint("Columns is empty [ ", errColumns, " ]"))
		}

		jsoResult, errRaw := rawcmds.GetRow(dataNodeItem.DBConn, sqlSelect, txtTable, columns, txtKeyname)
		if errRaw == nil {
			if jsoResult != nil {
				dbTable.Lock()
				dbDataItem = new(DataItem)
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
