package trans

import (
	"github.com/SERV4BIZ/gfp/jsons"
	"github.com/SERV4BIZ/hscale/api/utility"
)

func (me *HDBTx) Rollback() error {
	jsaNodeKey := jsons.JSONArrayFactory()
	me.HDB.MutexMapDataNode.RLock()
	for nodeKey := range me.HDB.MapDataNode {
		jsaNodeKey.PutString(nodeKey)
	}
	me.HDB.MutexMapDataNode.RUnlock()

	var errResult error = nil
	for jsaNodeKey.Length() > 0 {
		index := utility.RandomIntn(jsaNodeKey.Length())
		nodeKey := jsaNodeKey.GetString(index)
		jsaNodeKey.Remove(index)

		me.HDB.MutexMapDataNode.RLock()
		dataNodeItem, nodeOk := me.HDB.MapDataNode[nodeKey]
		me.HDB.MutexMapDataNode.RUnlock()

		if nodeOk {
			dataNodeItem.MutexMapDBTx.Lock()
			dbTx, dbTxOk := dataNodeItem.MapDBTx[me.UUID]
			if dbTxOk {
				errRoll := dbTx.Rollback()
				if errRoll != nil {
					errResult = errRoll
				}
				delete(dataNodeItem.MapDBTx, me.UUID)
			}
			dataNodeItem.MutexMapDBTx.Unlock()
		}
	}

	return errResult
}
