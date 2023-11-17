package compression

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
)

const (
	CompressLevel = 9
	gzipID1       = 0x1f
	gzipID2       = 0x8b
)

func IsCompressed(data []byte) bool {
	return data[0] == gzipID1 && data[1] == gzipID2
}

func CompressData(data []byte) ([]byte, error) {
	var b bytes.Buffer
	gz, err := gzip.NewWriterLevel(&b, CompressLevel)
	if err != nil {
		return nil, err
	}
	_, err = gz.Write(data)
	if err != nil {
		return nil, err
	}
	err = gz.Close()
	if err != nil {
		return nil, err
	}

	return b.Bytes(), nil
}

func SizeInMB(size int64) string {
	return fmt.Sprintf("%.1f MB", float32(size)/1024/1024)
}

func UncompressData(data []byte) ([]byte, error) {
	rd := bytes.NewReader(data)
	gr, err := gzip.NewReader(rd)
	if err != nil {
		return nil, err
	}

	defer gr.Close()
	return io.ReadAll(gr)
}
