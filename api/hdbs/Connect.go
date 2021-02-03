package hdbs

import (
	"errors"
	"fmt"
	"strings"

	"github.com/SERV4BIZ/escondb"
)

// Connect is access connection to database in host and return connection.
func (me *DataNode) Connect() error {
	me.RLock()
	driverName := me.JSODataBase.GetString("txt_driver")
	host := me.JSODataBase.GetString("txt_host")
	port := me.JSODataBase.GetInt("int_port")
	username := me.JSODataBase.GetString("txt_username")
	password := me.JSODataBase.GetString("txt_password")
	me.RUnlock()

	me.DBConn,errConn := escondb.Connect(driverName, host, port, username, password, me.HDB.DBName)
	if errConn != nil {
		if conn != nil {
			conn.Close()
		}
		me.DBConn = nil
		return errConn
	}
	me.DBConn = conn
	return nil
}
