package trans

import "github.com/SERV4BIZ/hscale/api/hscales/hdbs"

// HDBTx is transaction object
type HDBTx struct {
	HDB  *hdbs.HDB
	UUID string
}
