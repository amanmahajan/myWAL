package wal

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"log"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
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
	entry, err := res.getLastLogEntry()
	if err != nil {
		return nil, err
	}
	res.lastSeqNum = entry.SeqNumber

	// Firing a go routine that will sync continuously this WAL file to disk after the interval
	go res.keepSyncing()
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

// Sync This method flushes the in memory buffer data into disk.
// If the fsync is enabled
func (w *WAL) Sync() error {
	err := w.bufferW.Flush()
	if err != nil {
		return err
	}
	if w.shouldFsync {
		err := w.currSegment.Sync()
		if err != nil {
			return err
		}
	}
	w.resetTimer()
	return nil
}

func (w *WAL) resetTimer() {
	w.timerSync.Reset(SyncInterval)
}

/*
•
This case is triggered when a timer (w.timerSync) goes off.
The .C represents the channel associated with the timer, and when the timer fires, a value is received from this channel.
Purpose: Every time the timer triggers, the WAL will attempt to flush (sync) any buffered data to disk.
*/
func (w *WAL) keepSyncing() {
	for {
		select {
		case <-w.timerSync.C:
			w.mutex.Lock()
			err := w.Sync()
			w.mutex.Unlock()
			if err != nil {
				log.Printf("wal: syncing failed: %v", err)
			}
		case <-w.ctx.Done():
			return

		}

	}
}

///////////// WRITE ENTRY LOGIC

func (w *WAL) WriteEntry(data []byte, isCheckpoint bool) error {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	// Check if we  need to rotate the logs first

	err := w.RotateLogsIfSizeIsLarge()
	if err != nil {
		return err
	}

	w.lastSeqNum++
	entry := &Entry{

		SeqNumber: w.lastSeqNum,
		Data:      data,
		CRC:       crc32.ChecksumIEEE(append(data, byte(w.lastSeqNum))), // Appending last sequence no as a byte
	}

	if isCheckpoint {
		err := w.Sync()
		if err != nil {
			log.Printf("wal: checkpointing failed: %v", err)
		}

		entry.IsCheckpoint = &isCheckpoint
	}
	return w.writeEntryToBuffer(entry)

}

/*
This function writes a marshalled Entry object to a buffer in two parts:

	1.	Size: First, it writes the size (in bytes) of the marshalled entry. This allows the reader to know how much data to expect.
	2.	Data: Then, it writes the actual marshalled entry to the buffer.

*/

func (w *WAL) writeEntryToBuffer(entry *Entry) error {

	if entry == nil {
		return nil
	}

	marshalled := Marshall(entry)
	size := len(marshalled)
	// Write size of marshalled data
	if err := binary.Write(w.bufferW, binary.LittleEndian, size); err != nil {
		return err
	}
	/// Write the actual marshalled data
	_, err := w.bufferW.Write(marshalled)
	return err
}

func (w *WAL) RotateLogsIfSizeIsLarge() error {
	fileInfo, err := w.currSegment.Stat()
	if err != nil {
		return err
	}
	currSize := fileInfo.Size() + int64(w.bufferW.Size())
	if currSize > w.maxFileSize {
		// Rotate the log
		err := w.RotateLogs()
		if err != nil {
			return err
		}

	}
	return nil
}

func (w *WAL) RotateLogs() error {

	// Sync the current file
	err := w.Sync()
	if err != nil {
		return err
	}

	// Close the current file
	err = w.currSegment.Close()
	if err != nil {
		return err
	}

	w.currSegmentIdx++

	if w.currSegmentIdx >= w.maxSegmentSize {
		// Delete the old segment
		err := w.deleteOldestSegment()
		if err != nil {
			return err
		}
	}

	segmentFile, err := createSegmentFile(w.directory, w.currSegmentIdx)
	if err != nil {
		return err
	}
	w.currSegment = segmentFile
	w.bufferW = bufio.NewWriter(segmentFile)
	return nil

}

func (w *WAL) deleteOldestSegment() error {
	files, err := filepath.Glob(filepath.Join(w.directory, SegmentPrefix+"*"))
	if err != nil {
		return err
	}
	if len(files) == 0 {
		return nil
	}
	if len(files) > 0 {
		val, err := w.findTheOldestSegment(files)
		if err != nil {
			return err
		}
		err = os.Remove(val)
		if err != nil {
			return err
		}
	}
	return nil

}

func (w *WAL) findTheOldestSegment(files []string) (string, error) {
	oldIdx := math.MaxInt
	oldSegmentFilePath := ""

	for _, file := range files {
		valStr := strings.Trim(file, filepath.Join(w.directory, SegmentPrefix))
		idx, err := strconv.Atoi(valStr)
		if err != nil {
			return "", err
		}
		if idx < oldIdx {
			oldIdx = idx
			oldSegmentFilePath = file
		}
	}
	return oldSegmentFilePath, nil

}
