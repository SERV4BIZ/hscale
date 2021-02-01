package hdbs

import (
	"errors"
	"fmt"
	"strings"

	"github.com/SERV4BIZ/hscale/api/drivers/postgresql"
)

// Connect is access connection to database in host and return connection.
func Connect(dataNodeItem *DataNode) error {
	dataNodeItem.RLock()
	driverName := dataNodeItem.JSODataBase.GetString("txt_driver")
	host := dataNodeItem.JSODataBase.GetString("txt_host")
	port := dataNodeItem.JSODataBase.GetInt("int_port")
	username := dataNodeItem.JSODataBase.GetString("txt_username")
	password := dataNodeItem.JSODataBase.GetString("txt_password")
	dataNodeItem.RUnlock()

	if strings.ToLower(strings.TrimSpace(driverName)) == "postgresql" {
		conn, errConn := postgresql.Connect(host, port, username, password, dataNodeItem.HDB.DBName)
		if errConn != nil {
			if conn != nil {
				conn.Close()
			}
			dataNodeItem.DBConn = nil
			return errConn
		}
		dataNodeItem.DBConn = conn
		return nil
	}

	dataNodeItem.DBConn = nil
	return errors.New(fmt.Sprint("Not found ", driverName, " driver"))
}
