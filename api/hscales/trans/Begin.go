package trans

import (
	"errors"
	"fmt"

	"github.com/SERV4BIZ/gfp/jsons"
	"github.com/SERV4BIZ/gfp/uuid"
	"github.com/SERV4BIZ/hscale/api/hscales/hdbs"
	"github.com/SERV4BIZ/hscale/api/utilities"
)

// Begin is create transaction object
func Begin(hdbItem *hdbs.HDB) (*HDBTx, error) {
	txtUUID, errUUID := uuid.NewV4()
	if errUUID != nil {
		return nil, errors.New(fmt.Sprint("Can not generate uuid [ ", errUUID, " ]"))
	}

	nTxItem := new(HDBTx)
	nTxItem.HDB = hdbItem
	nTxItem.UUID = txtUUID

	jsaNodeKey := jsons.JSONArrayFactory()
	hdbItem.MutexMapDataNode.RLock()
	for nodeKey := range hdbItem.MapDataNode {
		jsaNodeKey.PutString(nodeKey)
	}
	hdbItem.MutexMapDataNode.RUnlock()

	for jsaNodeKey.Length() > 0 {
		index := utilities.RandomIntn(jsaNodeKey.Length())
		nodeKey := jsaNodeKey.GetString(index)
		jsaNodeKey.Remove(index)

		hdbItem.MutexMapDataNode.RLock()
		dataNodeItem := hdbItem.MapDataNode[nodeKey]
		hdbItem.MutexMapDataNode.RUnlock()

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
