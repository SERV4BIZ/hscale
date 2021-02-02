package trans

import (
	"errors"
	"strings"

	"github.com/SERV4BIZ/gfp/jsons"
	"github.com/SERV4BIZ/hscale/api/drivers/rawcmds"
	"github.com/SERV4BIZ/hscale/api/hscales/hdbs"
	"github.com/SERV4BIZ/hscale/api/utility"
)

// Counter is increase number of keyname
func (me *HDBTx) Counter(txtKeyname string) (int, error) {
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

		hdbs.Reconnect(dataNodeItem)
		dataNodeItem.RLock()
		sqlIncrease := dataNodeItem.JSOSQLDriver.GetString("increase_value")
		sqlSelect := dataNodeItem.JSOSQLDriver.GetString("select")
		dataNodeItem.RUnlock()

		if dataNodeItem.DBConn == nil {
			return -1, errors.New("Connection is not open")
		}

		dataNodeItem.MutexMapDBTx.Lock()
		txConn, txOk := dataNodeItem.MapDBTx[me.UUID]
		dataNodeItem.MutexMapDBTx.Unlock()

		if !txOk {
			return -1, errors.New("Connection transaction not found")
		}

		if txConn == nil {
			return -1, errors.New("Connection transaction not open")
		}

		errInc := rawcmds.IncreaseValue(txConn, sqlIncrease, txtTable, txtKeyname, txtColumn, 1)
		if errInc != nil {
			return -1, errInc
		}

		jsoResult, errRaw := rawcmds.GetRow(txConn, sqlSelect, txtTable, []string{"int_value"}, txtKeyname)
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

		hdbs.Reconnect(dataNodeItem)
		dataNodeItem.RLock()
		sqlIncrease := dataNodeItem.JSOSQLDriver.GetString("increase_value")
		sqlSelect := dataNodeItem.JSOSQLDriver.GetString("select")
		dataNodeItem.RUnlock()

		if dataNodeItem.DBConn == nil {
			return -1, errors.New("Connection is not open")
		}

		dataNodeItem.MutexMapDBTx.Lock()
		txConn, txOk := dataNodeItem.MapDBTx[me.UUID]
		dataNodeItem.MutexMapDBTx.Unlock()

		if !txOk {
			return -1, errors.New("Connection transaction not found")
		}

		if txConn == nil {
			return -1, errors.New("Connection transaction not open")
		}

		jsoResult, errRaw := rawcmds.GetRow(txConn, sqlSelect, txtTable, []string{"txt_keyname"}, txtKeyname)
		if errRaw == nil {
			if jsoResult != nil {
				errInc := rawcmds.IncreaseValue(txConn, sqlIncrease, txtTable, txtKeyname, txtColumn, 1)
				if errInc != nil {
					return -1, errInc
				}

				jsoResult, errRaw := rawcmds.GetRow(txConn, sqlSelect, txtTable, []string{"int_value"}, txtKeyname)
				if errRaw != nil {
					return -1, errRaw
				}

				dbTable.Lock()
				dbDataItem = new(hdbs.DataItem)
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

	hdbs.Reconnect(dataNodeItem)
	dataNodeItem.RLock()
	sqlInsert := dataNodeItem.JSOSQLDriver.GetString("insert")
	dataNodeItem.RUnlock()

	if dataNodeItem.DBConn == nil {
		return -1, errors.New("Connection is not open")
	}

	dataNodeItem.MutexMapDBTx.Lock()
	txConn, txOk := dataNodeItem.MapDBTx[me.UUID]
	dataNodeItem.MutexMapDBTx.Unlock()

	if !txOk {
		return -1, errors.New("Connection transaction not found")
	}

	if txConn == nil {
		return -1, errors.New("Connection transaction not open")
	}

	jsoData := jsons.JSONObjectFactory()
	jsoData.PutInt(txtColumn, 1)
	errInsert := rawcmds.InsertRow(txConn, sqlInsert, txtTable, txtKeyname, jsoData)
	if errInsert != nil {
		return -1, errInsert
	}

	dbTable.Lock()
	dbDataItem = new(hdbs.DataItem)
	dbDataItem.Name = txtKeyname
	dbDataItem.DataNode = dataNodeItem
	dbDataItem.DataTable = dbTable
	dbTable.MapDataItem[txtKeyname] = dbDataItem
	dbTable.Unlock()

	return 1, nil
}
