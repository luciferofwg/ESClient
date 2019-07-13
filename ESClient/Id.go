package ESClient

import (
	"Kafka2ESSever/global"
	"common"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

var id int64 = 0

var (
	filename string   = ""
	bRead    bool     = false
	fRead    *os.File = nil

	fWrite *os.File = nil
	bOpen  bool     = false
	wrLock sync.Mutex
)

func getCurPath() string {
	curPath, err := common.GetCurrentPath()
	if err != nil {
		return ""
	}
	return curPath + "/id"
}

func ReadId() bool {
	//文件已经读取
	if bRead {
		global.Log.Debug.Debug("id已经完成初始化，无需重新读取")
		return true
	}
	filename = getCurPath()
	if filename == "" {
		global.Log.Debug.Error("没有获取到可执行程序的目录")
		return false
	} else {
		global.Log.Debug.Info("文件 id 的目录是%s", filename)
	}

	//文件未读取
	if !exists(filename) {
		//文件不存在
		atomic.StoreInt64(&id, 0)
		bRead = true
		WriteId()
		global.Log.Debug.Debug("文件%s不存在,使用默认值，id=0", filename)
		return true
	}

	var err error
	fRead, err = os.OpenFile(filename, os.O_RDONLY, os.ModePerm) //打开文件
	if err != nil {
		atomic.StoreInt64(&id, 0)
		bRead = true
		WriteId()
		global.Log.Debug.Debug("读文件-打开文件%s失败，错误：%v,使用默认的值，id=0", filename, err)
		return true
	}
	defer fRead.Close()

	//位移到第一个字符处
	fRead.Seek(0, os.SEEK_SET)
	buf := make([]byte, 125)
	n, err := fRead.Read(buf)
	if err != nil {
		atomic.StoreInt64(&id, 0)
		bRead = true
		WriteId()
		global.Log.Debug.Debug("读取数据失败，错误：%v,使用默认的值id=0", err)
		return true
	}
	s := string(buf[0:n])
	if s == "" {
		atomic.StoreInt64(&id, 0)
		bRead = true
		WriteId()
		global.Log.Debug.Debug("读取数据%v,使用默认的值id=0", s)
		return true
	}
	s1 := strings.Trim(s, " ")
	tempId, err := strconv.Atoi(s1)
	if err != nil {
		atomic.StoreInt64(&id, 0)
		bRead = true
		WriteId()
		global.Log.Debug.Debug("转换id失败，错误：%v,使用默认的值id=0", err)
		return true
	}
	atomic.StoreInt64(&id, int64(tempId))
	global.Log.Debug.Info("读取成功，id=%d", id)
	return true
}

func WriteId() {
	if !bOpen {
		var err error
		fWrite, err = os.OpenFile(filename, os.O_WRONLY|os.O_CREATE, 0644)
		if err != nil {
			global.Log.Debug.Error("写文件-打开文件%s失败，错误：%v", filename, err)
		} else {
			bOpen = true
		}
	}
	wrLock.Lock()
	defer wrLock.Unlock()

	//得到字符的总长度
	size, err := fWrite.Seek(0, os.SEEK_END)
	//位移到第一个字符
	_, err = fWrite.Seek(0, os.SEEK_SET)
	//写空字符
	for i := 0; i < int(size); i++ {
		fWrite.WriteString(" ")
	}
	//位移到首部
	_, err = fWrite.Seek(0, os.SEEK_SET)
	//写s
	_, err = fWrite.WriteString(strconv.Itoa(int(atomic.LoadInt64(&id))))
	if err != nil {
		global.Log.Debug.Error("写数据%s到文件中失败，错误：%v", err)
	}
	global.Log.Debug.Info("写数据%d到文件中成功", atomic.LoadInt64(&id))
}

func exists(path string) bool {
	_, err := os.Stat(path)
	if err != nil {
		if os.IsExist(err) {
			return true
		}
		return false
	}
	return true
}
