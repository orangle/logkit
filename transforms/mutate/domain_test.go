package mutate

import (
	"testing"

	"github.com/qiniu/logkit/transforms"
	. "github.com/qiniu/logkit/utils/models"

	"github.com/stretchr/testify/assert"
)

func TestDomainTransformer(t *testing.T) {
	gsub := &Domain{
		Key:  "request",
		NewKey:  "domain",
	}
	data, err := gsub.Transform([]Data{{"request": "https://orangleliu.info/name/cc", "abc": "x1 y2"}, 
								{"request": "http://baidu.com/wenku/xx?name=cc", "abc": "x1"}})
	assert.NoError(t, err)

	exp := []Data{
		{"request": "https://orangleliu.info/name/cc", "abc": "x1 y2", "domain": "orangleliu.info"},
		{"request": "http://baidu.com/wenku/xx?name=cc", "abc": "x1", "domain": "baidu.com"},
	}
	t.Log(data)
	assert.Equal(t, exp, data)
	assert.Equal(t, gsub.Stage(), transforms.StageAfterParser)
}
