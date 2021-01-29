package trans

// AddList is same PutList function
func (me *HDBTx) AddList(txtTable string, txtKeyname string, txtHeadColumn string, txtItemKey string) error {
	return me.PutList(txtTable, txtKeyname, txtHeadColumn, txtItemKey)
}
