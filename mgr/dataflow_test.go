package mgr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/parser"
	"github.com/qiniu/logkit/utils"
	. "github.com/qiniu/logkit/utils/models"

	"github.com/json-iterator/go"
	"github.com/stretchr/testify/assert"
)

func Test_GetRawData(t *testing.T) {
	var testGetRawData = `{
    "name":"testGetRawData.csv",
    "batch_len": 3,
    "batch_size": 2097152,
    "batch_interval": 60,
    "batch_try_times": 3, 
    "reader":{
        "log_path":"./Test_GetRawData/logdir",
        "meta_path":"./Test_GetRawData1/meta_req_csv",
        "mode":"dir",
        "read_from":"oldest",
        "ignore_hidden":"true"
    }
}
`
	logfile := "./Test_GetRawData/logdir/log1"
	logdir := "./Test_GetRawData/logdir"
	if err := os.MkdirAll("./Test_GetRawData/confs1", 0777); err != nil {
		t.Error(err)
	}
	defer func() {
		os.RemoveAll("./Test_GetRawData")
		os.RemoveAll("./Test_GetRawData1")
	}()

	if err := os.MkdirAll(logdir, 0777); err != nil {
		t.Error(err)
	}
	err := createFile(logfile, 20000000)
	if err != nil {
		t.Error(err)
	}
	err = ioutil.WriteFile("./Test_GetRawData/confs1/test1.conf", []byte(testGetRawData), 0666)
	if err != nil {
		t.Error(err)
	}
	time.Sleep(1 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	confPathAbs, _, err := utils.GetRealPath("./Test_GetRawData/confs1/test1.conf")
	if err != nil {
		t.Error(err)
	}

	var runnerConf RunnerConfig
	err = conf.LoadEx(&runnerConf, confPathAbs)
	if err != nil {
		t.Error(err)
	}

	rawData, err := GetRawData(runnerConf.ReaderConfig)
	if err != nil {
		t.Error(err)
	}

	expected := "abc\n"
	assert.Equal(t, expected, rawData)
}

func Test_GetParsedData(t *testing.T) {
	c := conf.MapConf{}
	c[parser.KeyParserName] = "testparser"
	c[parser.KeyParserType] = "csv"
	c[parser.KeyCSVSchema] = "a long, b string, c float, d jsonmap,e date"
	c[parser.KeyCSVSplitter] = " "
	c[parser.KeyDisableRecordErrData] = "true"
	tmstr := time.Now().Format(time.RFC3339Nano)
	line1 := `1 fufu 3.14 {"x":1,"y":"2"} ` + tmstr + "\n"
	line2 := line1 + `cc jj uu {"x":1,"y":"2"} ` + tmstr + "\n"
	line3 := line2 + `2 fufu 3.15 999 ` + tmstr + "\n"
	line4 := line3 + `3 fufu 3.16 {"x":1,"y":["xx:12"]} ` + tmstr + "\n"
	line5 := line4 + `   ` + "\n"
	line6 := line5 + `4 fufu 3.17  ` + tmstr
	c[KeySampleLog] = line6
	parsedData, err := GetParsedData(c)
	if c, ok := err.(*utils.StatsError); ok {
		err = c.ErrorDetail
	}
	assert.Error(t, err)

	exp := make(map[string]interface{})
	exp["a"] = int64(1)
	exp["b"] = "fufu"
	exp["c"] = 3.14
	exp["d-x"] = json.Number("1")
	exp["d-y"] = "2"
	exp["e"] = tmstr
	for k, v := range parsedData[0] {
		if v != exp[k] {
			t.Errorf("expect %v but got %v", v, exp[k])
		}
	}
	expNum := 3
	assert.Equal(t, expNum, len(parsedData), fmt.Sprintln(parsedData))

	if parsedData[0]["a"] != int64(1) {
		t.Errorf("a should be 1  but got %v", parsedData[0]["a"])
	}
	if "fufu" != parsedData[0]["b"] {
		t.Error("b should be fufu")
	}
}

func TestGetTransformedData(t *testing.T) {
	config1 := `{
			"type":"IP",
			"key":  "ip",
			"data_path": "../transforms/ip/17monipdb.dat",
			"sampleLog": "{\"ip\": \"111.2.3.4\"}"
	}`

	var rc map[string]interface{}
	err := jsoniter.Unmarshal([]byte(config1), &rc)
	assert.NoError(t, err)

	transformData, err := GetTransformedData(rc)
	if err != nil {
		t.Error(err)
	}
	exp := []Data{{
		"ip":      "111.2.3.4",
		"Region":  "浙江",
		"City":    "宁波",
		"Country": "中国",
		"Isp":     "N/A"}}
	assert.Equal(t, exp, transformData)
}

func Test_getTransformerCreator(t *testing.T) {
	config1 := `{
			"type":"IP",
			"key":  "ip",
			"data_path": "../transforms/ip/17monipdb.dat",
			"sampleLog": "{\"ip\": \"111.2.3.4\"}"
	}`
	var rc map[string]interface{}
	err := jsoniter.Unmarshal([]byte(config1), &rc)
	assert.NoError(t, err)

	creator, err := getTransformerCreator(rc)
	if err != nil {
		t.Errorf("get transformer creator from transformer config fail, error : %v", err.Error())
	}

	if creator == nil {
		t.Errorf("expect get creator, but is nil")
	}
}

func Test_getDataFromTransformConfig(t *testing.T) {
	config1 := `{
			"type":"IP",
			"key":  "ip",
			"data_path": "../transforms/ip/17monipdb.dat",
			"sampleLog": "{\"ip\": \"111.2.3.4\"}"
	}`
	var rc map[string]interface{}
	err := jsoniter.Unmarshal([]byte(config1), &rc)
	assert.NoError(t, err)

	data, err := getDataFromTransformConfig(rc)
	if err != nil {
		t.Errorf("get data from transfomer config fail, error : %v", err.Error())
	}

	if len(data) != 1 {
		t.Errorf("expect 1 data but got %v", len(data))
	}

	for _, val := range data {
		assert.Equal(t, "111.2.3.4", val["ip"])
	}
}

func Test_getTransformer(t *testing.T) {
	config1 := `{
			"type":"IP",
			"key":  "ip",
			"data_path": "../transforms/ip/17monipdb.dat",
			"sampleLog": "{\"ip\": \"111.2.3.4\"}"
	}`
	var rc map[string]interface{}
	err := jsoniter.Unmarshal([]byte(config1), &rc)
	assert.NoError(t, err)

	creator, err := getTransformerCreator(rc)
	if err != nil {
		t.Errorf("get transformer creator from transformer config fail, error : %v", err.Error())
	}

	transformer, err := getTransformer(rc, creator)
	if err != nil {
		t.Errorf("get transformer from transformer config fail, error : %v", err.Error())
	}
	if transformer == nil {
		t.Error("expect get transformer but is empty")
	}
}