package global

import (
	"sync"

	"github.com/SERV4BIZ/gfp/jsons"
)

// AppName is application name
var AppName = "HSCALE DATABASE CONFIG"

// AppVersion is application version
var AppVersion = "1.0.0"

// CompanyName is name of company
var CompanyName = "SERV4BIZ CO.,LTD."

// DS is split for directory in path string
var DS = "/"

// MutexJSOConfig is mutex lock for JSOConfig object
var MutexJSOConfig sync.RWMutex

// JSOConfig is json object for application config file
var JSOConfig = jsons.JSONObjectFactory()

// MutexState is mutex lock for MemoryState
var MutexState sync.RWMutex

// MemoryState is size of memory program used
var MemoryState = 0

// LoadState is request per second
var LoadState = 0

// CountState is counting request per second
var CountState = 0

// Username is authen username
var Username = ""

// Password is authen password
var Password = ""
