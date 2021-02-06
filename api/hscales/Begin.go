package hscales

import (
	"errors"
	"fmt"

	"github.com/SERV4BIZ/gfp/jsons"
	"github.com/SERV4BIZ/gfp/uuid"
	"github.com/SERV4BIZ/hscale/api/utility"
)

// Begin is create transaction object
func (me *HDB) Begin() (*HDBTX, error) {
	txtUUID, errUUID := uuid.NewV4()
	if errUUID != nil {
		return nil, errors.New(fmt.Sprint("Can not generate uuid [ ", errUUID, " ]"))
	}

	nTxItem := new(HDBTX)
	nTxItem.HDB = me
	nTxItem.UUID = txtUUID

	jsaNodeKey := jsons.JSONArrayFactory()
	me.MutexMapDataNode.RLock()
	for nodeKey := range me.MapDataNode {
		jsaNodeKey.PutString(nodeKey)
	}
	me.MutexMapDataNode.RUnlock()

	for jsaNodeKey.Length() > 0 {
		index := utility.RandomIntn(jsaNodeKey.Length())
		nodeKey := jsaNodeKey.GetString(index)
		jsaNodeKey.Remove(index)

		me.MutexMapDataNode.RLock()
		dataNodeItem := me.MapDataNode[nodeKey]
		me.MutexMapDataNode.RUnlock()

		txItem, errTx := dataNodeItem.DBConn.Begin()
		if errTx != nil {
			txItem.Rollback()
			return nil, errors.New(fmt.Sprint("Can not begin transaction from node ", nodeKey, " [ ", errTx, " ] "))
		}

		dataNodeItem.MutexMapDBTx.Lock()
		dataNodeItem.MapDBTx[txtUUID] = txItem
		dataNodeItem.MutexMapDBTx.Unlock()
	}

	return nTxItem, nil
}
