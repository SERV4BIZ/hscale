package hscales

import (
	"errors"

	"github.com/SERV4BIZ/gfp/jsons"
	"github.com/SERV4BIZ/hscale/api/drivers"
	"github.com/SERV4BIZ/hscale/api/utility"
)

// DeleteRow is delete data aleady in database from node and shard id and keyname
func (me *HDBTX) DeleteRow(txtTable string, txtKeyname string) error {
	// Check in memory
	me.HDB.MutexMapDataTable.RLock()
	dbTable, tableOk := me.HDB.MapDataTable[txtTable]
	me.HDB.MutexMapDataTable.RUnlock()

	if !tableOk {
		return errors.New("Table not found")
	}

	dbTable.RLock()
	dbDataItem, itemOk := dbTable.MapDataItem[txtKeyname]
	dbTable.RUnlock()

	if itemOk {
		dataNodeItem := dbDataItem.DataNode

		dataNodeItem.Reconnect()
		if dataNodeItem.DBConn == nil {
			return errors.New("Connection is not open")
		}

		dataNodeItem.RLock()
		sqlDelete := dataNodeItem.JSOSQLDriver.GetString("delete")
		dataNodeItem.RUnlock()

		dataNodeItem.MutexMapDBTx.Lock()
		dbTx, dbTxOk := dataNodeItem.MapDBTx[me.UUID]
		dataNodeItem.MutexMapDBTx.Unlock()

		if !dbTxOk {
			return errors.New("Database transaction not found")
		}

		errRaw := drivers.DeleteRow(dbTx, sqlDelete, txtTable, txtKeyname)
		if errRaw != nil {
			return errRaw
		}

		dbTable.Lock()
		delete(dbTable.MapDataItem, txtKeyname)
		dbTable.Unlock()
		return nil
	}

	// If pointer not found
	me.HDB.MutexMapDataNode.RLock()
	jsaNodeKey := jsons.JSONArrayFactory()
	for key := range me.HDB.MapDataNode {
		jsaNodeKey.PutString(key)
	}
	me.HDB.MutexMapDataNode.RUnlock()

	for jsaNodeKey.Length() > 0 {
		index := utility.RandomIntn(jsaNodeKey.Length())
		nodeName := jsaNodeKey.GetString(index)
		jsaNodeKey.Remove(index)

		me.HDB.MutexMapDataNode.RLock()
		dataNodeItem := me.HDB.MapDataNode[nodeName]
		me.HDB.MutexMapDataNode.RUnlock()

		dataNodeItem.Reconnect()
		if dataNodeItem.DBConn == nil {
			return errors.New("Connection is not open")
		}

		dataNodeItem.RLock()
		sqlDelete := dataNodeItem.JSOSQLDriver.GetString("delete")
		dataNodeItem.RUnlock()

		dataNodeItem.MutexMapDBTx.Lock()
		dbTx, dbTxOk := dataNodeItem.MapDBTx[me.UUID]
		dataNodeItem.MutexMapDBTx.Unlock()

		if !dbTxOk {
			return errors.New("Database transaction not found")
		}

		errRaw := drivers.DeleteRow(dbTx, sqlDelete, txtTable, txtKeyname)
		if errRaw == nil {
			return nil
		}
	}
	return errors.New("Row not found")
}
