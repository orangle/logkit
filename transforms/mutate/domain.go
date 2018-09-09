package mutate

import (
	"fmt"
	"errors"
	"net/url"

	"github.com/qiniu/logkit/transforms"
	. "github.com/qiniu/logkit/utils/models"
)

var (
	_ transforms.StatsTransformer = &Domain{}
	_ transforms.Transformer      = &Domain{}
)

type Domain struct {
	Key       string `json:"key"`
	NewKey     string `json:"newfield"`
	stats     StatsInfo
}

func (g *Domain) RawTransform(datas []string) ([]string, error) {
	return datas, errors.New("domain transformer not support rawTransform")
}

func (g *Domain) Transform(datas []Data) ([]Data, error) {
	errNum := 0

	for i := range datas {
		//parse domain, add new key to data
		keys := GetKeys(g.Key)
		val, getErr := GetMapValue(datas[i], keys...)
		if getErr != nil {
			transforms.SetError(errNum, getErr, transforms.GetErr, g.Key)
			continue
		}

		strVal, ok := val.(string)
		if !ok {
			typeErr := fmt.Errorf("transform key %v data type is not string", g.Key)
			transforms.SetError(errNum, typeErr, transforms.General, "")
			continue
		}
		
		setErr := SetMapValue(datas[i], g.FetchDomain(strVal), false, g.NewKey)
		if setErr != nil {
			transforms.SetError(errNum, setErr, transforms.SetErr, g.NewKey)
		}
	}
	g.stats, _ = transforms.SetStatsInfo(nil, g.stats, 0, int64(len(datas)), g.Type())
	return datas, nil
}

func (g *Domain) FetchDomain(uri string) string {
	u, err := url.Parse(uri)
	if err != nil {
		return ""
	}
	return u.Hostname()
}

func (g *Domain) Description() string {
	return `提取URL格式的字段，赋值给信字段 http://sss.com/api/name, 提取出来sss.com`
}

func (g *Domain) Type() string {
	return "domain"
}

func (g *Domain) SampleConfig() string {
	return `{
		"type":"split",
		"key":"DomainFieldKey",
		"newfield":"name"
	}`
}

func (g *Domain) ConfigOptions() []Option {
	return []Option{
		transforms.KeyFieldName,
		{
			KeyName:      "newfield",
			ChooseOnly:   false,
			Default:      "",
			Required:     true,
			Placeholder:  "new_field_keyname",
			DefaultNoUse: true,
			Description:  "解析后数据的字段名(newfield)",
			Type:         transforms.TransformTypeString,
		},
	}
}

func (g *Domain) Stage() string {
	return transforms.StageAfterParser
}

func (g *Domain) Stats() StatsInfo {
	return g.stats
}

func (g *Domain) SetStats(err string) StatsInfo {
	g.stats.LastError = err
	return g.stats
}

func init() {
	transforms.Add("domain", func() transforms.Transformer {
		return &Domain{}
	})
}
