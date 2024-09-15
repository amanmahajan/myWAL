package wal

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const (
	SegmentPrefix = "segment-"
	SyncInterval  = 200 * time.Millisecond
)

type WAL struct {
	ctx            context.Context
	cancel         context.CancelFunc
	directory      string
	mutex          sync.Mutex
	lastSeqNum     uint64
	bufferW        *bufio.Writer
	timerSync      *time.Timer
	shouldFsync    bool
	maxFileSize    int64
	maxSegmentSize int
	currSegmentIdx int
	currSegment    *os.File
}

func OpenWal(fileDir string, enableSync bool, maxFileSize int64, maxSegmentSize int) (*WAL, error) {
	ctx, cancel := context.WithCancel(context.Background())

	// open the directory if it is available
	err := os.MkdirAll(fileDir, 0755)
	if err != nil {
		return nil, err
	}

	// Find all files that has prefix segment- in the given directory
	files, err := filepath.Glob(filepath.Join(fileDir, SegmentPrefix+"*"))
	if err != nil {
		return nil, err
	}

	lastSegmentIdx := 0

	if len(files) > 0 {
		// Find the last segment in the list of files
		lastSegmentIdx, err = findLastSegmentIndex(files)
		if err != nil {
			return nil, err
		}
	} else {

		file, err := createSegmentFile(fileDir, 0)
		if err != nil {
			return nil, err
		}
		err = file.Close()
		if err != nil {
			return nil, err
		}

	}

	filePath := filepath.Join(fileDir, fmt.Sprintf("%s%d", SegmentPrefix, lastSegmentIdx))

	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0644)

	if err != nil {
		return nil, err
	}

	// Go to the end of the  file
	if _, err = file.Seek(0, io.SeekEnd); err != nil {
		return nil, err
	}

	res := &WAL{
		ctx:            ctx,
		cancel:         cancel,
		directory:      fileDir,
		lastSeqNum:     0,
		bufferW:        bufio.NewWriter(file),
		timerSync:      time.NewTimer(SyncInterval),
		shouldFsync:    enableSync,
		maxFileSize:    maxFileSize,
		maxSegmentSize: maxSegmentSize,
		currSegmentIdx: lastSegmentIdx,
		currSegment:    file,
	}

	// set last sequence number

	return res, nil

}

/*
The function iterates through the log file, reading each entry’s size.
  - It keeps track of the offset and size of the last valid entry it encounters.
  - When it reaches the end of the file (io.EOF), it seeks back to the last valid entry using the stored offset and reads its data.
  - The function then unmarshals and verifies the entry, and finally returns the last valid entry in the log.
*/
func (w *WAL) getLastLogEntry(wal *WAL)
