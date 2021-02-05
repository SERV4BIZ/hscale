package hdbs

import (
	"errors"
	"strings"

	"github.com/SERV4BIZ/gfp/jsons"
	"github.com/SERV4BIZ/hscale/api/drivers"
	"github.com/SERV4BIZ/hscale/api/utility"
)

// Counter is increase number of keyname
func (me *HDBTX) Counter(txtKeyname string) (int, error) {
	if strings.TrimSpace(txtKeyname) == "" {
		return -1, errors.New("Data is empty")
	}
	txtTable := "counters"
	txtColumn := "int_value"

	me.HDB.MutexMapDataTable.RLock()
	dbTable, tableOk := me.HDB.MapDataTable[txtTable]
	me.HDB.MutexMapDataTable.RUnlock()

	if !tableOk {
		return -1, errors.New("Table not found")
	}

	dbTable.RLock()
	dbDataItem, itemOk := dbTable.MapDataItem[txtKeyname]
	dbTable.RUnlock()

	if itemOk {
		dataNodeItem := dbDataItem.DataNode

		dataNodeItem.Reconnect()
		if dataNodeItem.DBConn == nil {
			return -1, errors.New("Connection is not open")
		}
		
		dataNodeItem.RLock()
		sqlIncrease := dataNodeItem.JSOSQLDriver.GetString("increase_value")
		sqlSelect := dataNodeItem.JSOSQLDriver.GetString("select")
		dataNodeItem.RUnlock()

		dataNodeItem.MutexMapDBTx.Lock()
		dbTx, dbTxOk := dataNodeItem.MapDBTx[me.UUID]
		dataNodeItem.MutexMapDBTx.Unlock()

		if !dbTxOk {
			return -1, errors.New("Database transaction not found")
		}

		errInc := drivers.IncreaseValue(dbTx, sqlIncrease, txtTable, txtKeyname, txtColumn, 1)
		if errInc != nil {
			return -1, errInc
		}

		jsoResult, errRaw := drivers.GetRow(dbTx, sqlSelect, txtTable, []string{"int_value"}, txtKeyname)
		if errRaw != nil {
			return -1, errRaw
		}

		return jsoResult.GetInt("int_value"), nil
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

		dataNodeItem.Reconnect()
		if dataNodeItem.DBConn == nil {
			return -1, errors.New("Connection is not open")
		}

		dataNodeItem.RLock()
		sqlIncrease := dataNodeItem.JSOSQLDriver.GetString("increase_value")
		sqlSelect := dataNodeItem.JSOSQLDriver.GetString("select")
		dataNodeItem.RUnlock()

		dataNodeItem.MutexMapDBTx.Lock()
		dbTx, dbTxOk := dataNodeItem.MapDBTx[me.UUID]
		dataNodeItem.MutexMapDBTx.Unlock()

		if !dbTxOk {
			return -1, errors.New("Database transaction not found")
		}

		jsoResult, errRaw := drivers.GetRow(dbTx, sqlSelect, txtTable, []string{"txt_keyname"}, txtKeyname)
		if errRaw == nil {
			if jsoResult != nil {
				errInc := drivers.IncreaseValue(dbTx, sqlIncrease, txtTable, txtKeyname, txtColumn, 1)
				if errInc != nil {
					return -1, errInc
				}

				jsoResult, errRaw := drivers.GetRow(dbTx, sqlSelect, txtTable, []string{"int_value"}, txtKeyname)
				if errRaw != nil {
					return -1, errRaw
				}

				dbTable.Lock()
				dbDataItem = new(DataItem)
				dbDataItem.Name = txtKeyname
				dbDataItem.DataNode = dataNodeItem
				dbDataItem.DataTable = dbTable
				dbTable.MapDataItem[txtKeyname] = dbDataItem
				dbTable.Unlock()

				return jsoResult.GetInt("int_value"), errRaw
			}
		}
	}

	// If not found then insert row
	me.HDB.MutexMapDataNode.RLock()
	dataNodeItem := me.HDB.MapDataNode[nodeKeys[utility.RandomIntn(len(nodeKeys))]]
	me.HDB.MutexMapDataNode.RUnlock()

	dataNodeItem.Reconnect()
	if dataNodeItem.DBConn == nil {
		return -1, errors.New("Connection is not open")
	}

	dataNodeItem.RLock()
	sqlInsert := dataNodeItem.JSOSQLDriver.GetString("insert")
	dataNodeItem.RUnlock()

	dataNodeItem.MutexMapDBTx.Lock()
	dbTx, dbTxOk := dataNodeItem.MapDBTx[me.UUID]
	dataNodeItem.MutexMapDBTx.Unlock()

	if !dbTxOk {
		return -1, errors.New("Database transaction not found")
	}

	jsoData := jsons.JSONObjectFactory()
	jsoData.PutInt(txtColumn, 1)
	errInsert := drivers.InsertRow(dbTx, sqlInsert, txtTable, txtKeyname, jsoData)
	if errInsert != nil {
		return -1, errInsert
	}

	dbTable.Lock()
	dbDataItem = new(DataItem)
	dbDataItem.Name = txtKeyname
	dbDataItem.DataNode = dataNodeItem
	dbDataItem.DataTable = dbTable
	dbTable.MapDataItem[txtKeyname] = dbDataItem
	dbTable.Unlock()

	return 1, nil
}
