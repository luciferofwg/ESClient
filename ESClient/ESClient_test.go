package ESClient

import (
	"strconv"
	"testing"
)

func TestNewESClient(t *testing.T) {
	var docs []interface{} = nil
	for i := 0; i < 10; i++ {
		m := make(map[string]interface{})
		m[strconv.Itoa(i)] = "hello, " + strconv.Itoa(i)
		docs = append(docs, m)
	}
}
