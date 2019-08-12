package ESClient

import (
	"github.com/satori/go.uuid"
	"os"
	"path/filepath"
	"runtime"
	"strings"
)

func GetProecessName() string {
	switch strOs := runtime.GOOS; strOs {
	case "windows":
		{
			proName := filepath.Base(os.Args[0])
			byteName := []byte(proName)
			return string(byteName[0:strings.LastIndex(proName, ".")])
		}
	case "linux":
		{
			proName := filepath.Base(os.Args[0])
			return proName
		}
	default:
		return ""
	}
}

func GenetateUUID() string {
	u1, _ := uuid.NewV4()
	return u1.String()
}

func isExist(path string) bool {
	_, err := os.Stat(path)
	if err != nil {
		if os.IsExist(err) {
			return true
		}
		return false
	}
	return true
}
