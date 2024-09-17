package wal

import (
	"errors"
	"google.golang.org/protobuf/proto"
	"hash/crc32"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

/*
Create a new segment file in a given directory
fileDir : directory path
id : segmentId
*/
func createSegmentFile(fileDir string, id int) (*os.File, error) {
	filePath := filepath.Join(fileDir, SegmentPrefix+strconv.Itoa(id))
	file, err := os.Create(filePath)
	if err != nil {
		return nil, err
	}
	return file, nil

}

func findLastSegmentIndex(files []string) (int, error) {

	currIdx := 0
	for _, filename := range files {
		segmentId, err := strconv.Atoi(strings.TrimPrefix(filename, SegmentPrefix))
		if err != nil {
			return 0, err
		}
		if segmentId > currIdx {
			currIdx = segmentId
		}
	}
	return currIdx, nil

}

func deserializeAndCheckCRC(data []byte) (*Entry, error) {
	var entry *Entry
	UnMarshall(entry, data)
	if !verifyCRC(entry) {
		return nil, errors.New("invalid CRC")
	}
	return entry, nil

}

func verifyCRC(entry *Entry) bool {
	val := crc32.ChecksumIEEE(append(entry.GetData(), byte(entry.GetSeqNumber())))
	return val == entry.GetCRC()
}

func Marshall(entry *Entry) []byte {
	val, err := proto.Marshal(entry)
	if err != nil {
		panic(err)
	}

	return val
}

func UnMarshall(entry *Entry, data []byte) {
	err := proto.Unmarshal(data, entry)
	if err != nil {
		panic(err)
	}
}
