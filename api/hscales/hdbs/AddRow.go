package hdbs

import (
	"errors"

	"github.com/SERV4BIZ/gfp/jsons"
	"github.com/SERV4BIZ/hscale/api/drivers/rawcmds"
	"github.com/SERV4BIZ/hscale/api/utilities"
)

// AddRow is add new data in database from node and shard id and keyname
func (me *HDB) AddRow(txtTable string, txtKeyname string, jsoData *jsons.JSONObject) error {
	if jsoData.Length() <= 0 {
		return errors.New("Data is empty")
	}

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
		sqlSelect := dataNodeItem.JSOSQLDriver.GetString("select")
		dataNodeItem.RUnlock()

		if dataNodeItem.DBConn == nil {
			return errors.New("Connection is not open")
		}

		_, errRaw := rawcmds.GetRow(dataNodeItem.DBConn, sqlSelect, txtTable, []string{"txt_keyname"}, txtKeyname)
		if errRaw == nil {
			return errors.New("Already in database")
		}
	}

	// If not found data pointer
	// Find aleady data in any node if found then update it
	me.MutexMapDataNode.RLock()
	jsaNodeKey := jsons.JSONArrayFactory()
	nodeKeys := make([]string, 0)
	for key := range me.MapDataNode {
		jsaNodeKey.PutString(key)
		nodeKeys = append(nodeKeys, key)
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
		dataNodeItem.RUnlock()

		if dataNodeItem.DBConn == nil {
			return errors.New("Connection is not open")
		}

		jsoResult, errRaw := rawcmds.GetRow(dataNodeItem.DBConn, sqlSelect, txtTable, []string{"txt_keyname"}, txtKeyname)
		if errRaw == nil {
			if jsoResult != nil {
				dbTable.Lock()
				dbDataItem = new(DataItem)
				dbDataItem.Name = txtKeyname
				dbDataItem.DataNode = dataNodeItem
				dbDataItem.DataTable = dbTable
				dbTable.MapDataItem[txtKeyname] = dbDataItem
				dbTable.Unlock()
				return errors.New("Already in database")
			}
		}
	}

	// If not found then insert row
	me.MutexMapDataNode.RLock()
	dataNodeItem := me.MapDataNode[nodeKeys[utilities.RandomIntn(len(nodeKeys))]]
	me.MutexMapDataNode.RUnlock()

	Reconnect(dataNodeItem)
	dataNodeItem.RLock()
	sqlInsert := dataNodeItem.JSOSQLDriver.GetString("insert")
	dataNodeItem.RUnlock()

	if dataNodeItem.DBConn == nil {
		return errors.New("Connection is not open")
	}

	return rawcmds.InsertRow(dataNodeItem.DBConn, sqlInsert, txtTable, txtKeyname, jsoData)
}
