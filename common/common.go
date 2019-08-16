package common

import (
	"io"
	"io/ioutil"
	"os"
	"reflect"
	"unsafe"
)

// Args tuple.
type Args struct {
	Database      string
	Outdir        string
	ExcludeTables string
	Threads       int
	ChunksizeInMB int
	StmtSize      int
	Allbytes      uint64
	Allrows       uint64

	// Interval in millisecond.
	IntervalMs int
}

// BytesToString casts slice to string without copy
func BytesToString(b []byte) (s string) {
	if len(b) == 0 {
		return ""
	}

	bh := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	sh := reflect.StringHeader{Data: bh.Data, Len: bh.Len}

	return *(*string)(unsafe.Pointer(&sh))
}

// StringToBytes casts string to slice without copy
func StringToBytes(s string) []byte {
	if len(s) == 0 {
		return []byte{}
	}

	sh := (*reflect.StringHeader)(unsafe.Pointer(&s))
	bh := reflect.SliceHeader{Data: sh.Data, Len: sh.Len, Cap: sh.Len}

	return *(*[]byte)(unsafe.Pointer(&bh))
}

// WriteFile used to write datas to file.
func WriteFile(file string, data string) error {
	flag := os.O_RDWR | os.O_TRUNC
	if _, err := os.Stat(file); os.IsNotExist(err) {
		flag |= os.O_CREATE
	}
	f, err := os.OpenFile(file, flag, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	n, err := f.Write(StringToBytes(data))
	if err != nil {
		return err
	}
	if n != len(data) {
		return io.ErrShortWrite
	}
	return nil
}

// ReadFile used to read datas from file.
func ReadFile(file string) ([]byte, error) {
	return ioutil.ReadFile(file)
}

// AssertNil used to assert the error.
func AssertNil(err error) {
	if err != nil {
		panic(err)
	}
}

func EscapeString(v string) string {
	var pos = 0
	if len(v) == 0 {
		return ""
	}
	buf := make([]byte, len(v[:])*2)
	for i := 0; i < len(v); i++ {
		c := v[i]
		switch c {
		case '\x00':
			buf[pos] = '\\'
			buf[pos+1] = '0'
			pos += 2
		case '\n':
			buf[pos] = '\\'
			buf[pos+1] = 'n'
			pos += 2
		case '\r':
			buf[pos] = '\\'
			buf[pos+1] = 'r'
			pos += 2
		case '\x1a':
			buf[pos] = '\\'
			buf[pos+1] = 'Z'
			pos += 2
		case '\'':
			buf[pos] = '\\'
			buf[pos+1] = '\''
			pos += 2
		case '"':
			buf[pos] = '\\'
			buf[pos+1] = '"'
			pos += 2
		case '\\':
			buf[pos] = '\\'
			buf[pos+1] = '\\'
			pos += 2
		default:
			buf[pos] = c
			pos++
		}
	}
	return string(buf[:pos])
}
