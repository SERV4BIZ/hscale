package hdbs

// AddList is same PutList function
func (me *HDBTX) AddList(txtTable string, txtKeyname string, txtHeadColumn string, txtItemKey string) error {
	return me.PutList(txtTable, txtKeyname, txtHeadColumn, txtItemKey)
}