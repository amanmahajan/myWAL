package wal

import (
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
