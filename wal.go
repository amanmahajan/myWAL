package wal

import (
	"bufio"
	"context"
	"encoding/binary"
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

func (w *WAL) getLastLogEntry() (*Entry, error) {
	file, err := os.OpenFile(w.currSegment.Name(), os.O_RDONLY, 0644)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var lastSize int32
	var offset int64
	var entry *Entry

	for {
		var currSize int32

		if err := binary.Read(file, binary.BigEndian, &currSize); err != nil {
			if err == io.EOF {
				// Handle End of file Err
				if offset == 0 {
					// no offset found
					return entry, nil
				}
				// 	If the offset is not 0, it seeks to the last known position (offset) where the last valid entry was recorded.
				if _, err := file.Seek(offset, io.SeekStart); err != nil {
					return nil, err
				}
				// Handle reading of data of the size

				data := make([]byte, lastSize)
				if _, err := io.ReadFull(file, data); err != nil {
					return nil, err
				}

				entry, err = deserializeAndCheckCRC(data)
				if err != nil {
					return nil, err
				}
				return entry, nil

			}
		}

		// Get current offset
		offset, err = file.Seek(0, io.SeekCurrent)
		lastSize = currSize

		if err != nil {
			return nil, err
		}

		// Skip to the next entry.
		if _, err := file.Seek(int64(currSize), io.SeekCurrent); err != nil {
			return nil, err
		}
	}
}
