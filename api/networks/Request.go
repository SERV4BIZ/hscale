package networks

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/SERV4BIZ/gfp/handler"
	"github.com/SERV4BIZ/gfp/jsons"
)

// Request is command send package to host
func Request(jsoHost, jsoCmd *jsons.JSONObject) *jsons.JSONObject {
	jsoRequest := jsons.JSONObjectFactory()
	jsoRequest.PutInt("status", 0)

	url := fmt.Sprint(jsoHost.GetString("txt_protocol"), "://", jsoHost.GetString("txt_host"), ":", jsoHost.GetInt("int_port"), "/")
	req, errReq := http.NewRequest("POST", url, bytes.NewBuffer([]byte(jsoCmd.ToString())))
	if handler.Error(errReq) {
		jsoRequest.PutString("txt_msg", fmt.Sprint("Can not create post request [ ", errReq, " ]"))
		return jsoRequest
	}

	req.Header.Set("Content-Type", "application/json")
	defer req.Body.Close()

	intTime := jsoHost.GetInt("int_timeout")
	if intTime <= 0 {
		intTime = 15
	}

	timeout := time.Duration(time.Duration(intTime) * time.Second)
	client := &http.Client{Timeout: timeout}
	resp, errResp := client.Do(req)
	if handler.Error(errResp) {
		jsoRequest.PutString("txt_msg", fmt.Sprint("Response is error [ ", errResp, " ]"))
		return jsoRequest
	}
	defer resp.Body.Close()

	if resp.StatusCode == 200 {
		body, errBody := ioutil.ReadAll(resp.Body)
		if handler.Error(errBody) {
			jsoRequest.PutString("txt_msg", fmt.Sprint("Can not read body of response [ ", errBody, " ]"))
			return jsoRequest
		}

		jsoResult, errResult := jsons.JSONObjectFromString(string(body))
		if handler.Error(errResult) {
			jsoRequest.PutString("txt_msg", fmt.Sprint("Can not parse json data [ ", errResult, " ]"))
			return jsoRequest
		}

		return jsoResult
	}

	jsoRequest.PutString("txt_msg", fmt.Sprint("Response is not successfully status error code ", resp.StatusCode))
	return jsoRequest
}
