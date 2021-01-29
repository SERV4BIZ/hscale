package trans

import (
	"errors"

	"github.com/SERV4BIZ/gfp/jsons"
	"github.com/SERV4BIZ/hscale/api/drivers/rawcmds"
	"github.com/SERV4BIZ/hscale/api/hscales/hdbs"
	"github.com/SERV4BIZ/hscale/api/utility"
)

// PutRow is put data in database from node and shard id and keyname
func (me *HDBTx) PutRow(txtTable string, txtKeyname string, jsoData *jsons.JSONObject) error {
	if jsoData.Length() <= 0 {
		return errors.New("Data is empty")
	}

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
		sqlUpdate := dataNodeItem.JSOSQLDriver.GetString("update")
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

		return rawcmds.UpdateRow(txConn, sqlUpdate, txtTable, txtKeyname, jsoData)
	}

	// If not found data pointer
	// Find aleady data in any node if found then update it
	me.HDB.MutexMapDataNode.RLock()
	jsaNodeKey := jsons.JSONArrayFactory()
	nodeKeys := make([]string, 0)
	for key := range me.HDB.MapDataNode {
		jsaNodeKey.PutString(key)
		nodeKeys = append(nodeKeys, key)
	}
	me.HDB.MutexMapDataNode.RUnlock()

	for jsaNodeKey.Length() > 0 {
		index := utility.RandomIntn(jsaNodeKey.Length())
		nodeName := jsaNodeKey.GetString(index)
		jsaNodeKey.Remove(index)

		me.HDB.MutexMapDataNode.RLock()
		dataNodeItem := me.HDB.MapDataNode[nodeName]
		me.HDB.MutexMapDataNode.RUnlock()

		hdbs.Reconnect(dataNodeItem)
		dataNodeItem.RLock()
		sqlSelect := dataNodeItem.JSOSQLDriver.GetString("select")
		sqlUpdate := dataNodeItem.JSOSQLDriver.GetString("update")
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

				return rawcmds.UpdateRow(txConn, sqlUpdate, txtTable, txtKeyname, jsoData)
			}
		}
	}

	// If not found then insert row
	me.HDB.MutexMapDataNode.RLock()
	dataNodeItem := me.HDB.MapDataNode[nodeKeys[utility.RandomIntn(len(nodeKeys))]]
	me.HDB.MutexMapDataNode.RUnlock()

	hdbs.Reconnect(dataNodeItem)
	dataNodeItem.RLock()
	sqlInsert := dataNodeItem.JSOSQLDriver.GetString("insert")
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

	return rawcmds.InsertRow(txConn, sqlInsert, txtTable, txtKeyname, jsoData)
}
