package hdbs

import (
	"errors"

	"github.com/SERV4BIZ/gfp/jsons"
	"github.com/SERV4BIZ/hscale/api/drivers/rawcmds"
	"github.com/SERV4BIZ/hscale/api/utility"
)

// DeleteRow is delete data aleady in database from node and shard id and keyname
func (me *HDB) DeleteRow(txtTable string, txtKeyname string) error {
	// Check in memory
	me.MutexMapDataTable.RLock()
	dbTable, tableOk := me.MapDataTable[txtTable]
	me.MutexMapDataTable.RUnlock()

	if !tableOk {
		return errors.New("Table not found")
	}

	dbTable.RLock()
	dbDataItem, itemOk := dbTable.MapDataItem[txtKeyname]
	dbTable.RUnlock()

	if itemOk {
		dataNodeItem := dbDataItem.DataNode

		Reconnect(dataNodeItem)
		dataNodeItem.RLock()
		sqlDelete := dataNodeItem.JSOSQLDriver.GetString("delete")
		dataNodeItem.RUnlock()

		if dataNodeItem.DBConn == nil {
			return errors.New("Connection is not open")
		}

		errRaw := rawcmds.DeleteRow(dataNodeItem.DBConn, sqlDelete, txtTable, txtKeyname)
		if errRaw != nil {
			return errRaw
		}

		dbTable.Lock()
		delete(dbTable.MapDataItem, txtKeyname)
		dbTable.Unlock()
		return nil
	}

	// If pointer not found
	me.MutexMapDataNode.RLock()
	jsaNodeKey := jsons.JSONArrayFactory()
	for key := range me.MapDataNode {
		jsaNodeKey.PutString(key)
	}
	me.MutexMapDataNode.RUnlock()

	for jsaNodeKey.Length() > 0 {
		index := utility.RandomIntn(jsaNodeKey.Length())
		nodeName := jsaNodeKey.GetString(index)
		jsaNodeKey.Remove(index)

		me.MutexMapDataNode.RLock()
		dataNodeItem := me.MapDataNode[nodeName]
		me.MutexMapDataNode.RUnlock()

		Reconnect(dataNodeItem)
		dataNodeItem.RLock()
		sqlDelete := dataNodeItem.JSOSQLDriver.GetString("delete")
		dataNodeItem.RUnlock()

		if dataNodeItem.DBConn == nil {
			return errors.New("Connection is not open")
		}

		errRaw := rawcmds.DeleteRow(dataNodeItem.DBConn, sqlDelete, txtTable, txtKeyname)
		if errRaw == nil {
			return nil
		}
	}
	return errors.New("Row not found")
}
