package main

import (
	"flag"
	"fmt"
	"time"
)

var (
	addr         = flag.String("ip", "127.0.0.1", "elasticsearch ip addr,like(127.0.0.1)")
	templateName = flag.String("template", "", "elasticsearch template name,like(person)")
	tpyName      = flag.String("typ", "doc", "elasticsearch typ name, default 'doc'")

	path   = flag.String("file", "", "the filename need to transmition into elasticsearch")
	splite = flag.Int("splite", 0, "file content splite,like 0[\t];1[space];2[,] ")
)

func main() {
	flag.Parse()
	if *path == "" {
		fmt.Println("文件路径不能为空")
		return
	}
	if *templateName == "" {
		*templateName = time.Now().Format("2006-01-02 15:04:05")[0:13]
	}
	fmt.Println(*path, *splite, *addr, *templateName, *tpyName)
	s := ""
	switch *splite {
	case 0:
		s = "\t"
	case 1:
		s = " "
	case 2:
		s = ","
	default:
		s = "\t"

	}
	t := NewTableFile(*path, s, *addr, *templateName, *tpyName)
	t.Start()
}
