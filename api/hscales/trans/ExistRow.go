package trans

import (
	"errors"

	"github.com/SERV4BIZ/gfp/jsons"
	"github.com/SERV4BIZ/hscale/api/drivers/rawcmds"
	"github.com/SERV4BIZ/hscale/api/hscales/hdbs"
	"github.com/SERV4BIZ/hscale/api/utilities"
)

// ExistRow is check data aleady in database from node and shard id and keyname
func (me *HDBTx) ExistRow(txtTable string, txtKeyname string) error {
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

		hdbs.Reconnect(dataNodeItem)
		dataNodeItem.RLock()
		sqlSelect := dataNodeItem.JSOSQLDriver.GetString("select")
		dataNodeItem.RUnlock()

		if dataNodeItem.DBConn == nil {
			return errors.New("Connection is not open")
		}

		dataNodeItem.MutexMapDBTx.Lock()
		txConn, txOk := dataNodeItem.MapDBTx[me.UUID]
		dataNodeItem.MutexMapDBTx.Unlock()

		if !txOk {
			return errors.New("Connection transaction not found")
		}

		if txConn == nil {
			return errors.New("Connection transaction not open")
		}

		_, errRaw := rawcmds.GetRow(txConn, sqlSelect, txtTable, []string{"txt_keyname"}, txtKeyname)
		return errRaw
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
		dataNodeItem.RUnlock()

		if dataNodeItem.DBConn == nil {
			return errors.New("Connection is not open")
		}

		dataNodeItem.MutexMapDBTx.Lock()
		txConn, txOk := dataNodeItem.MapDBTx[me.UUID]
		dataNodeItem.MutexMapDBTx.Unlock()

		if !txOk {
			return errors.New("Connection transaction not found")
		}

		if txConn == nil {
			return errors.New("Connection transaction not open")
		}

		jsoResult, errRaw := rawcmds.GetRow(txConn, sqlSelect, txtTable, []string{"txt_keyname"}, txtKeyname)
		if errRaw == nil {
			if jsoResult != nil {
				dbTable.Lock()
				dbDataItem = new(hdbs.DataItem)
				dbDataItem.Name = txtKeyname
				dbDataItem.DataNode = dataNodeItem
				dbDataItem.DataTable = dbTable
				dbTable.MapDataItem[txtKeyname] = dbDataItem
				dbTable.Unlock()
				return nil
			}
		}
	}
	return errors.New("Row not found")
}
