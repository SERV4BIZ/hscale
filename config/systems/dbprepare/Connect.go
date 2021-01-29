package dbprepare

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/SERV4BIZ/gfp/handler"
	"github.com/SERV4BIZ/hscale/config/systems/drivers/postgresql"
)

// Connect is access connection to database in host and return connection.
func Connect(driverName string, host string, port int, username string, password string, dbname string) (*sql.DB, error) {
	if strings.ToLower(driverName) == "postgresql" {
		conn, errConn := postgresql.Connect(host, port, username, password, dbname)
		if conn != nil && !handler.Error(errConn) {
			return conn, errConn
		}
		return nil, errors.New(fmt.Sprint("Can't connect to database host [] ", errConn, " ]"))
	}
	return nil, errors.New("Driver not found")
}
