package hdbs

import (
	"errors"
	"strings"

	"github.com/SERV4BIZ/gfp/jsons"
	"github.com/SERV4BIZ/hscale/api/drivers/rawcmds"
	"github.com/SERV4BIZ/hscale/api/utility"
)

// Counter is increase number of keyname
func (me *HDB) Counter(txtKeyname string) (int, error) {
	if strings.TrimSpace(txtKeyname) == "" {
		return -1, errors.New("Data is empty")
	}
	txtTable := "counters"
	txtColumn := "int_value"

	me.MutexMapDataTable.RLock()
	dbTable, tableOk := me.MapDataTable[txtTable]
	me.MutexMapDataTable.RUnlock()

	if !tableOk {
		return -1, errors.New("Table not found")
	}

	dbTable.RLock()
	dbDataItem, itemOk := dbTable.MapDataItem[txtKeyname]
	dbTable.RUnlock()

	if itemOk {
		dataNodeItem := dbDataItem.DataNode

		Reconnect(dataNodeItem)
		dataNodeItem.RLock()
		sqlIncrease := dataNodeItem.JSOSQLDriver.GetString("increase_value")
		sqlSelect := dataNodeItem.JSOSQLDriver.GetString("select")
		dataNodeItem.RUnlock()

		if dataNodeItem.DBConn == nil {
			return -1, errors.New("Connection is not open")
		}

		errInc := rawcmds.IncreaseValue(dataNodeItem.DBConn, sqlIncrease, txtTable, txtKeyname, txtColumn, 1)
		if errInc != nil {
			return -1, errInc
		}

		jsoResult, errRaw := rawcmds.GetRow(dataNodeItem.DBConn, sqlSelect, txtTable, []string{"int_value"}, txtKeyname)
		if errRaw != nil {
			return -1, errRaw
		}

		return jsoResult.GetInt("int_value"), nil
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
		index := utility.RandomIntn(jsaNodeKey.Length())
		nodeName := jsaNodeKey.GetString(index)
		jsaNodeKey.Remove(index)

		me.MutexMapDataNode.RLock()
		dataNodeItem := me.MapDataNode[nodeName]
		me.MutexMapDataNode.RUnlock()

		Reconnect(dataNodeItem)
		dataNodeItem.RLock()
		sqlIncrease := dataNodeItem.JSOSQLDriver.GetString("increase_value")
		sqlSelect := dataNodeItem.JSOSQLDriver.GetString("select")
		dataNodeItem.RUnlock()

		if dataNodeItem.DBConn == nil {
			return -1, errors.New("Connection is not open")
		}

		jsoResult, errRaw := rawcmds.GetRow(dataNodeItem.DBConn, sqlSelect, txtTable, []string{"txt_keyname"}, txtKeyname)
		if errRaw == nil {
			if jsoResult != nil {
				errInc := rawcmds.IncreaseValue(dataNodeItem.DBConn, sqlIncrease, txtTable, txtKeyname, txtColumn, 1)
				if errInc != nil {
					return -1, errInc
				}

				jsoResult, errRaw := rawcmds.GetRow(dataNodeItem.DBConn, sqlSelect, txtTable, []string{"int_value"}, txtKeyname)
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
	me.MutexMapDataNode.RLock()
	dataNodeItem := me.MapDataNode[nodeKeys[utility.RandomIntn(len(nodeKeys))]]
	me.MutexMapDataNode.RUnlock()

	Reconnect(dataNodeItem)
	dataNodeItem.RLock()
	sqlInsert := dataNodeItem.JSOSQLDriver.GetString("insert")
	dataNodeItem.RUnlock()

	if dataNodeItem.DBConn == nil {
		return -1, errors.New("Connection is not open")
	}

	jsoData := jsons.JSONObjectFactory()
	jsoData.PutInt(txtColumn, 1)
	errInsert := rawcmds.InsertRow(dataNodeItem.DBConn, sqlInsert, txtTable, txtKeyname, jsoData)
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
