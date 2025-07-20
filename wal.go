package kv3

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"math/rand"
	"os"
	"sort"
	"time"

	"github.com/aergoio/kv3/varint"
)

// WAL constants
const (
	// WAL magic string for identification
	WalMagicString = "KV3WAL"
	// WAL header size
	WalHeaderSize = 28
	// WAL frame header size
	WalFrameHeaderSize = 25
)

// WAL frame types
const (
	WalFrameTypePage   = 'P' // Page frame (index file)
	WalFrameTypeValue  = 'V' // Value frame (values file)
	WalFrameTypeCommit = 'C' // Commit frame
)

// WalInfo represents the WAL file information
type WalInfo struct {
	file           *os.File
	salt1          uint32
	salt2          uint32
	walPath        string
	hasher         *crc32.Table // CRC32 table for checksum calculations
	checksum       uint32       // Current cumulative checksum
	lastCommitChecksum uint32   // Checksum value at the last commit
	lastCommitPosition int64    // Position just after the last valid commit
	nextWritePosition  int64    // Position where the next frame will be written
	lastCommitSequence int64    // Sequence number of the last committed transaction
	lastCheckpointSequence int64 // Sequence number of the last checkpointed transaction
}

// ------------------------------------------------------------------------------------------------
// Local WAL cache (used only at initialization when scanning the WAL file)
// ------------------------------------------------------------------------------------------------

// WalPageEntry represents a page entry in the WAL cache
type WalPageEntry struct {
	PageNumber     uint32     // Page number of the page in the index file
	Data           []byte     // The page data
	SequenceNumber int64      // Transaction sequence number when this page was written
	Next           *WalPageEntry // Pointer to the next entry with the same page number (for rollback)
}

// localCache handles operations on the local WAL page cache
type localCache map[uint32]*WalPageEntry

// add adds a page to the WAL cache
func (cache localCache) add(pageNumber uint32, data []byte, commitSequence int64) {
	// Check if there's already an entry for this page from the current transaction
	existingEntry, exists := cache[pageNumber]

	if exists && existingEntry.SequenceNumber == commitSequence {
		// Page from current transaction already exists, replace its data
		existingEntry.Data = data
		return
	}

	// Create a new entry with the current sequence number
	newEntry := &WalPageEntry{
		PageNumber:     pageNumber,
		Data:           data,
		SequenceNumber: commitSequence,
		Next:           existingEntry, // Link to previous version if it exists
	}

	// Update the cache with the new entry as the head of the list
	cache[pageNumber] = newEntry
}

// discardNewerPages removes pages from the current transaction from the cache
func (cache localCache) discardNewerPages(currentSeq int64) {
	// Iterate through all pages in the cache
	for pageNumber, entry := range cache {
		// Find the first entry that's not from the current transaction
		var newHead *WalPageEntry = entry
		for newHead != nil && newHead.SequenceNumber == currentSeq {
			newHead = newHead.Next
		}
		// Update the cache with the new head (or delete if no valid entries remain)
		if newHead != nil {
			cache[pageNumber] = newHead
		} else {
			delete(cache, pageNumber)
		}
	}
}

// discardOldPageVersions removes older versions of pages after a commit
func (cache localCache) discardOldPageVersions() {
	// Iterate through all pages in the cache
	for _, entry := range cache {
		// Keep only the most recent version (head of the list) and remove older versions
		if entry != nil && entry.Next != nil {
			// This page is from the current transaction being committed, keep it and remove older versions
			entry.Next = nil
		}
	}
}

// ------------------------------------------------------------------------------------------------
// WAL file operations
// ------------------------------------------------------------------------------------------------

// writeToWAL writes an index page to the WAL file
func (db *DB) writeToWAL(pageData []byte, pageNumber uint32) error {
	// Check if we're in WAL mode
	if !db.useWAL {
		return nil
	}
	// Open or create the WAL file if it doesn't exist
	if db.walInfo == nil {
		err := db.openWAL()
		if err != nil {
			return err
		}
	}

	// Write the page frame to the WAL file
	err := db.writePageFrame(pageNumber, pageData)
	if err != nil {
		return err
	}

	return nil
}

// createWAL creates a new WAL file
func (db *DB) createWAL() error {
	var salt1 uint32  // this salt is incremented on each WAL reset
	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	var lastCommitSequence int64 = 0
	var lastCheckpointSequence int64 = 0

	if db.walInfo == nil {
		// Generate a new salt1 for a new WAL file
		salt1 = r.Uint32()
	} else {
		// Use the salt1 from the existing WAL file
		salt1 = db.walInfo.salt1 + 1 // increment the salt1 on each WAL reset
		lastCommitSequence = db.walInfo.lastCommitSequence
		lastCheckpointSequence = db.walInfo.lastCheckpointSequence
	}

	// Generate WAL file path by appending "-wal" to the main db file path
	walPath := db.filePath + "-wal"

	// Open or create the WAL file
	walFile, err := os.OpenFile(walPath, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return fmt.Errorf("failed to create WAL file: %w", err)
	}

	// Initialize WAL info
	db.walInfo = &WalInfo{
		file:      walFile,
		walPath:   walPath,
		hasher:    crc32.IEEETable,
		checksum:  0,
	}

	// Initialize the WAL info
	db.walInfo.salt1 = salt1
	db.walInfo.salt2 = r.Uint32()
	db.walInfo.lastCommitPosition = WalHeaderSize // For a new file, commit position is right after header
	db.walInfo.nextWritePosition = WalHeaderSize  // Start writing after the header
	db.walInfo.lastCommitSequence = lastCommitSequence
	db.walInfo.lastCheckpointSequence = lastCheckpointSequence

	// Create header buffer
	header := make([]byte, WalHeaderSize)

	// Write magic string
	copy(header[0:6], WalMagicString)

	// Write version (currently 1)
	binary.BigEndian.PutUint16(header[6:8], 1)

	// Write salts
	binary.BigEndian.PutUint32(header[8:12], db.walInfo.salt1)
	binary.BigEndian.PutUint32(header[12:16], db.walInfo.salt2)

	// Write database ID
	binary.BigEndian.PutUint64(header[16:24], db.databaseID)

	// Calculate checksum for header (first 24 bytes)
	checksum := crc32.ChecksumIEEE(header[0:24])
	binary.BigEndian.PutUint32(header[24:28], checksum)

	// Store the header checksum
	db.walInfo.checksum = checksum

	// Write header to WAL file
	if _, err := db.walInfo.file.WriteAt(header, 0); err != nil {
		return fmt.Errorf("failed to write WAL header: %w", err)
	}

	// Sync if in full sync mode
	if db.syncMode == SyncOn {
		if err := db.walInfo.file.Sync(); err != nil {
			return fmt.Errorf("failed to sync WAL file: %w", err)
		}
	}

	return nil
}

// writeFrameHeader writes a frame header to the WAL file
func (db *DB) writeFrameHeader(frameType byte, identifier uint32, offset int64, data []byte) (int64, error) {

	// Use the nextWritePosition instead of getting the file size
	frameOffset := db.walInfo.nextWritePosition

	// Create frame header buffer
	frameHeader := make([]byte, WalFrameHeaderSize)

	// Write frame type
	frameHeader[0] = frameType

	// Write identifier (page number for pages, unused for values and commits)
	binary.BigEndian.PutUint32(frameHeader[1:5], identifier)

	// Write offset (for values) or zero (for pages and commits)
	binary.BigEndian.PutUint64(frameHeader[5:13], uint64(offset))

	// Write salts
	binary.BigEndian.PutUint32(frameHeader[13:17], db.walInfo.salt1)
	binary.BigEndian.PutUint32(frameHeader[17:21], db.walInfo.salt2)

	// Calculate cumulative checksum
	// Start with the current checksum value
	checksum := db.walInfo.checksum

	// Update checksum with first 13 bytes of frame header (type, identifier, offset)
	checksum = crc32.Update(checksum, db.walInfo.hasher, frameHeader[0:13])

	// Update checksum with data if provided
	if data != nil {
		checksum = crc32.Update(checksum, db.walInfo.hasher, data)
	}

	// Store the new checksum
	db.walInfo.checksum = checksum

	// Write the checksum to the frame header
	binary.BigEndian.PutUint32(frameHeader[21:25], checksum)

	// Write frame header to WAL file
	if _, err := db.walInfo.file.WriteAt(frameHeader, frameOffset); err != nil {
		return 0, fmt.Errorf("failed to write WAL frame header: %w", err)
	}

	return frameOffset, nil
}

// writePageFrame writes a page frame to the WAL file
func (db *DB) writePageFrame(pageNumber uint32, pageData []byte) error {
	// Write the frame header
	frameOffset, err := db.writeFrameHeader(WalFrameTypePage, pageNumber, 0, pageData)
	if err != nil {
		return err
	}

	// Write page data after the header
	if _, err := db.walInfo.file.WriteAt(pageData, frameOffset+WalFrameHeaderSize); err != nil {
		return fmt.Errorf("failed to write WAL page frame data: %w", err)
	}

	// Update the nextWritePosition for the next frame
	db.walInfo.nextWritePosition = frameOffset + WalFrameHeaderSize + int64(len(pageData))

	return nil
}

// writeValueFrame writes a value frame to the WAL file
func (db *DB) writeValueFrame(offset int64, value []byte) error {
	var valueData []byte
	if offset == FreeValuesListOffset {
		// use the raw value data
		valueData = value
	} else {
		// Prepare the value data with type, size, and value
		valueData = prepareValueData(value)
	}

	// Write the frame header with the total segment size in the identifier field
	frameOffset, err := db.writeFrameHeader(WalFrameTypeValue, uint32(len(valueData)), offset, valueData)
	if err != nil {
		return err
	}

	// Write value data after the header
	if _, err := db.walInfo.file.WriteAt(valueData, frameOffset+WalFrameHeaderSize); err != nil {
		return fmt.Errorf("failed to write WAL value frame data: %w", err)
	}

	// Update the nextWritePosition for the next frame
	db.walInfo.nextWritePosition = frameOffset + WalFrameHeaderSize + int64(len(valueData))

	return nil
}

// writeValueToWAL writes a value to the WAL file
func (db *DB) writeValueToWAL(offset int64, value []byte) error {
	// Check if we're in WAL mode
	if !db.useWAL {
		return nil
	}
	// Open or create the WAL file if it doesn't exist
	if db.walInfo == nil {
		err := db.openWAL()
		if err != nil {
			return err
		}
	}

	// Write the value frame to the WAL file
	err := db.writeValueFrame(offset, value)
	if err != nil {
		return err
	}

	return nil
}

// prepareValueData creates a properly formatted value data buffer with type, size, and value
func prepareValueData(value []byte) []byte {
	// If the value is nil, mark it as deleted
	if value == nil {
		return []byte{'F'}
	}

	// Calculate the exact size needed for the value data
	varintSize := varint.Size(uint64(len(value)))
	totalSize := 1 + varintSize + len(value) // 1 for type, varint size, value size

	// Create the complete value data: type + size + value
	valueData := make([]byte, totalSize)
	valueData[0] = 'V' // Type

	// Write size as varint directly into the buffer
	varint.Write(valueData[1:], uint64(len(value)))

	// Copy value data
	copy(valueData[1+varintSize:], value)

	return valueData
}

// scanWAL loads valid frames from the WAL file and populates the in-memory cache
func (db *DB) scanWAL() error {
	if db.walInfo == nil {
		return nil
	}

	// Get WAL file size
	walFileInfo, err := db.walInfo.file.Stat()
	if err != nil {
		return fmt.Errorf("failed to get WAL file size: %w", err)
	}

	// If file is empty or only has a header, there's nothing to scan
	if walFileInfo.Size() <= WalHeaderSize {
		// TODO: check if the header is valid
		db.walInfo.lastCommitPosition = WalHeaderSize
		db.walInfo.nextWritePosition = WalHeaderSize
		return nil
	}

	debugPrint("Scanning WAL file\n")

	// Read WAL header first
	headerBuf := make([]byte, WalHeaderSize)
	if _, err := db.walInfo.file.ReadAt(headerBuf, 0); err != nil {
		return fmt.Errorf("failed to read WAL header: %w", err)
	}

	// Verify magic string
	if string(headerBuf[0:6]) != WalMagicString {
		return fmt.Errorf("invalid WAL file: magic string mismatch")
	}

	// Verify header checksum
	headerChecksum := binary.BigEndian.Uint32(headerBuf[24:28])
	calculatedHeaderChecksum := crc32.ChecksumIEEE(headerBuf[0:24])
	if headerChecksum != calculatedHeaderChecksum {
		return fmt.Errorf("invalid WAL file: header checksum mismatch")
	}

	// Extract salts from header
	db.walInfo.salt1 = binary.BigEndian.Uint32(headerBuf[8:12])
	db.walInfo.salt2 = binary.BigEndian.Uint32(headerBuf[12:16])

	// Extract database ID from header
	walDatabaseID := binary.BigEndian.Uint64(headerBuf[16:24])

	// Check if database ID matches
	if walDatabaseID != db.databaseID {
		// Database ID mismatch, reset the WAL file
		debugPrint("WAL database ID mismatch: %d vs %d, resetting WAL file\n", walDatabaseID, db.databaseID)
		// Reset the WAL file
		return db.resetWAL()
	}

	// Initialize page cache as a local variable
	localCache := make(localCache)

	// Start reading frames from after the header
	offset := int64(WalHeaderSize)
	lastCommitOffset := offset // Initialize to just after header
	commitSequence := int64(1)

	// Initialize running checksum for frame validation
	runningChecksum := headerChecksum
	lastCommitChecksum := headerChecksum

	for offset+WalFrameHeaderSize <= walFileInfo.Size() {
		// Read frame header
		frameHeader := make([]byte, WalFrameHeaderSize)
		if _, err := db.walInfo.file.ReadAt(frameHeader, offset); err != nil {
			// Error reading frame header, stop scanning
			break
		}

		// Extract frame type
		frameType := frameHeader[0]

		// Verify salt values match the header
		frameSalt1 := binary.BigEndian.Uint32(frameHeader[13:17])
		frameSalt2 := binary.BigEndian.Uint32(frameHeader[17:21])
		if frameSalt1 != db.walInfo.salt1 || frameSalt2 != db.walInfo.salt2 {
			// Salt mismatch, stop scanning
			debugPrint("Salt mismatch: %d vs %d, %d vs %d\n", frameSalt1, db.walInfo.salt1, frameSalt2, db.walInfo.salt2)
			break
		}

		// Extract the frame checksum
		frameChecksum := binary.BigEndian.Uint32(frameHeader[21:25])

		// Calculate expected checksum
		// Update running checksum with first 13 bytes of frame header (type, identifier, offset)
		runningChecksum = crc32.Update(runningChecksum, db.walInfo.hasher, frameHeader[0:13])

		if frameType == WalFrameTypeCommit {
			// This is a commit record, update lastCommitOffset
			// Commit records have no data, just the header
			debugPrint("Processing commit record for transaction %d\n", commitSequence)

			// Verify checksum
			if frameChecksum != runningChecksum {
				// Checksum mismatch, stop scanning
				debugPrint("Commit checksum mismatch: %d vs %d\n", frameChecksum, runningChecksum)
				break
			}

			// Update the last commit offset
			lastCommitOffset = offset + WalFrameHeaderSize

			// Store the checksum from the commit record
			lastCommitChecksum = frameChecksum

			// Clean up old page versions after commit
			localCache.discardOldPageVersions()

			// Checkpoint the content of this commit
			if !db.readOnly {
				// Copy these pages and values to the files
				db.copyPagesToIndexFile(localCache, commitSequence)
				db.copyValuesToValuesFile(commitSequence)
			}

			// Increment the commit sequence number
			commitSequence++

			// Move to the next frame
			offset = lastCommitOffset
			continue
		}

		if frameType == WalFrameTypePage {
			// This is a page frame
			// Ensure we don't read past the end of the file
			pageSize := int64(PageSize)
			if offset+WalFrameHeaderSize+pageSize > walFileInfo.Size() {
				debugPrint("Page frame past end of file: %d vs %d\n", offset+WalFrameHeaderSize+pageSize, walFileInfo.Size())
				break
			}

			// Extract page number from frame header
			pageNumber := binary.BigEndian.Uint32(frameHeader[1:5])

			// Read the page data
			pageData := make([]byte, pageSize)
			if _, err := db.walInfo.file.ReadAt(pageData, offset+WalFrameHeaderSize); err != nil {
				// Error reading page data, stop scanning
				debugPrint("Error reading page data: %v\n", err)
				break
			}

			// Update running checksum with page data
			runningChecksum = crc32.Update(runningChecksum, db.walInfo.hasher, pageData)

			// Verify checksum
			if frameChecksum != runningChecksum {
				// Checksum mismatch, stop scanning
				debugPrint("Page checksum mismatch: %d vs %d\n", frameChecksum, runningChecksum)
				break
			}

			debugPrint("Loading page %d from WAL to cache\n", pageNumber)

			// Add the page directly to the in-memory cache
			localCache.add(pageNumber, pageData, commitSequence)

			// Move to the next frame
			offset += WalFrameHeaderSize + pageSize

		} else if frameType == WalFrameTypeValue {
			// This is a value frame
			// Extract the value offset from frame header
			valueOffset := int64(binary.BigEndian.Uint64(frameHeader[5:13]))

			// Extract the total segment size from the identifier field
			segmentSize := int(binary.BigEndian.Uint32(frameHeader[1:5]))

			// Ensure we don't read past the end of the file
			if offset+WalFrameHeaderSize+int64(segmentSize) > walFileInfo.Size() {
				debugPrint("Value frame past end of file: %d vs %d\n", offset+WalFrameHeaderSize+int64(segmentSize), walFileInfo.Size())
				break
			}

			// Read the complete value data
			segment := make([]byte, segmentSize)
			if _, err := db.walInfo.file.ReadAt(segment, offset+WalFrameHeaderSize); err != nil {
				debugPrint("Error reading value data: %v\n", err)
				break
			}

			// Update running checksum with value data
			runningChecksum = crc32.Update(runningChecksum, db.walInfo.hasher, segment)

			// Verify checksum
			if frameChecksum != runningChecksum {
				// Checksum mismatch, stop scanning
				debugPrint("Value checksum mismatch: %d vs %d\n", frameChecksum, runningChecksum)
				break
			}

			// Get the value type
			valueType := segment[0]

			// Extract the actual value based on type
			var actualValue []byte
			if valueOffset == FreeValuesListOffset {
				// use the raw value data
				actualValue = segment
			} else if valueType == 'F' {
				// Deleted value - store as nil
				actualValue = nil
			} else if valueType == 'V' {
				// Parse the actual value from the value data (skip type and size varint)
				if segmentSize < 2 {
					// Invalid value data size
					debugPrint("Invalid value data size: %d\n", segmentSize)
					break
				}
				// Parse the size varint to find where the actual value starts
				valueSize64, bytesRead := varint.Read(segment[1:])
				if bytesRead == 0 || 1+bytesRead+int(valueSize64) != segmentSize {
					// Invalid value data format
					debugPrint("Invalid value data format: %d\n", segmentSize)
					break
				}
				// Normal value - extract the data (skip type and size)
				actualValue = segment[1+bytesRead:]
			} else {
				// Unknown value type, skip this frame
				debugPrint("Unknown value type: %d\n", valueType)
				break
			}

			debugPrint("Loading value (offset: %d) from WAL to cache\n", valueOffset)

			// Add the value to the value cache with versioning
			db.valueCacheMutex.Lock()
			existingEntry := db.valueCache[valueOffset]
			newEntry := &ValueEntry{
				value:       actualValue,
				txnSequence: commitSequence,
				isWAL:       true,
				next:        existingEntry, // Link to previous version
			}
			db.valueCache[valueOffset] = newEntry
			db.valueCacheMutex.Unlock()

			// Move to the next frame
			offset += WalFrameHeaderSize + int64(segmentSize)

		} else {
			// Unknown frame type, stop scanning
			debugPrint("Unknown frame type: %d\n", frameType)
			break
		}
	}

	// If the last scanned position is beyond the last commit position, there were frames without a commit
	// In this case, we need to remove all pages from the uncommitted transaction from the cache
	if offset > lastCommitOffset {
		// Remove pages and values from the current uncommitted transaction from the cache
		localCache.discardNewerPages(commitSequence)
		db.discardNewerValues(commitSequence)
	}

	// Update the position fields
	db.walInfo.lastCommitPosition = lastCommitOffset
	db.walInfo.nextWritePosition = lastCommitOffset

	// Store the final checksum value
	db.walInfo.lastCommitChecksum = lastCommitChecksum
	db.walInfo.checksum = lastCommitChecksum

	// Update the transaction sequence number
	db.txnSequence = commitSequence
	db.walInfo.lastCommitSequence = commitSequence
	db.walInfo.lastCheckpointSequence = commitSequence

	// Transfer the cached pages to the global page cache
	for pageNumber, entry := range localCache {
		if entry.Data[0] == ContentTypeRadix {
			_, err := db.parseRadixPage(entry.Data, pageNumber)
			if err != nil {
				return fmt.Errorf("failed to parse radix page: %w", err)
			}
		} else if entry.Data[0] == ContentTypeLeaf {
			_, err := db.parseLeafPage(entry.Data, pageNumber)
			if err != nil {
				return fmt.Errorf("failed to parse leaf page: %w", err)
			}
		} else if pageNumber == 0 {
			_, err := db.parseHeaderPage(entry.Data)
			if err != nil {
				return fmt.Errorf("failed to parse header page: %w", err)
			}
		} else {
			return fmt.Errorf("unknown page type: %d", entry.Data[0])
		}
	}

	if !db.readOnly {
		// Reset the WAL file as all content was checkpointed
		return db.resetWAL()
	}

	// Update the index file size to account for WAL pages
	return db.RefreshFileSize()
}

// walCommit writes a commit record to the WAL file
func (db *DB) walCommit(isCallerThread bool) error {
	// If WAL info is not initialized, return
	if db.walInfo == nil {
		return nil
	}

	// Write a commit record - just a header with no data
	frameOffset, err := db.writeFrameHeader(WalFrameTypeCommit, 0, 0, nil)
	if err != nil {
		return err
	}

	// Update the next write position (only header, no data for commit records)
	db.walInfo.nextWritePosition = frameOffset + WalFrameHeaderSize

	// Update the lastCommitPosition to the current nextWritePosition
	db.walInfo.lastCommitPosition = db.walInfo.nextWritePosition

	// Store the current checksum as the last committed checksum
	db.walInfo.lastCommitChecksum = db.walInfo.checksum

	// Sync if in full sync mode
	if db.syncMode == SyncOn {
		// Sync the WAL file
		if err := db.walInfo.file.Sync(); err != nil {
			return fmt.Errorf("failed to sync WAL file after commit: %w", err)
		}
	}

	// Update sequence number after successful commit
	db.walInfo.lastCommitSequence = db.txnSequence

	// Check if it should run a checkpoint
	if db.shouldCheckpoint(isCallerThread) {
		// Checkpoint the WAL file into the index file
		if err := db.checkpointWAL(); err != nil {
			// Log error but don't fail the commit
			debugPrint("Checkpoint failed: %v", err)
		}
	// If the commit is made by the main thread, clean up the cache
	} else if db.commitMode == CallerThread {
		// Clean up old page versions from cache
		db.discardOldPageVersions(true)
		// Discard previous versions of values
		db.discardOldValueVersions(true)
	}

	return nil
}

/*
// walRollback rolls back the current transaction
func (db *DB) walRollback() error {
	if db.walInfo == nil {
		return nil
	}

	// Reset the write position to the last commit position
	db.walInfo.nextWritePosition = db.walInfo.lastCommitPosition

	// Restore checksum to the value from the last commit
	db.walInfo.checksum = db.walInfo.lastCommitChecksum

	// Get current sequence number before rollback
	currentSeq := db.txnSequence

	// Remove pages that are from the current transaction from the cache
	db.discardNewerPages(currentSeq)

	// Remove values that are from the current transaction from the value cache
	db.rollbackValueCache(currentSeq)

	// Keep the same sequence number for the next transaction attempt
	// This is important to maintain consistency

	return nil
}
*/

// shouldCheckpoint checks if the WAL file should be checkpointed
func (db *DB) shouldCheckpoint(isCallerThread bool) bool {
	// If the database is closing, don't checkpoint
	if db.isClosing {
		return false
	}

	// If the commit is made by the main thread, only checkpoint if the commit mode is CallerThread
	if isCallerThread && db.commitMode != CallerThread {
		return false
	}

	// Check WAL file size
	walFileInfo, err := db.walInfo.file.Stat()
	if err == nil {
		// Checkpoint if WAL file exceeds size threshold
		if walFileInfo.Size() > db.checkpointThreshold {
			return true
		}
	}
	return false
}

// checkpointWAL writes the current WAL file to the index file and clears the cache
func (db *DB) checkpointWAL() error {
	if db.walInfo == nil {
		return nil
	}

	debugPrint("Checkpoint WAL. Last commit sequence: %d\n", db.walInfo.lastCommitSequence)

	// Lock the cache during checkpoint
	//db.walInfo.cacheMutex.Lock()

	// If not already synced on walCommit
	if db.syncMode == SyncOff {
		// Sync the WAL file to ensure all changes are persisted
		if err := db.walInfo.file.Sync(); err != nil {
			return fmt.Errorf("failed to sync WAL file before checkpoint: %w", err)
		}
	}

	// Get the start sequence number for the checkpoint
	//startSequence := db.walInfo.lastCheckpointSequence

	// Copy WAL content to the keys file and values file
	if err := db.copyWALContentToDbFiles(); err != nil {
		return fmt.Errorf("failed to copy pages to index file: %w", err)
	}

	// Sync the destination files to ensure all changes are persisted
	if err := db.valuesFile.Sync(); err != nil {
		return fmt.Errorf("failed to sync values file after checkpoint: %w", err)
	}
	if err := db.indexFile.Sync(); err != nil {
		return fmt.Errorf("failed to sync index file after checkpoint: %w", err)
	}

	// Update the last checkpoint sequence number
	db.walInfo.lastCheckpointSequence = db.walInfo.lastCommitSequence + 1

	// Clear the page cache
	// Clear the isWAL flag for all pages and remove older versions
	db.discardOldPageVersions(false)
	// Discard previous versions of values
	db.discardOldValueVersions(false)

	// Reset the WAL file
	return db.resetWAL()
}

// copyPagesToIndexFile copies pages from the WAL cache to the index file
// only pages with the specified commit sequence number or higher are copied
func (db *DB) copyPagesToIndexFile(localCache localCache, commitSequence int64) error {
	if db.walInfo == nil || localCache == nil {
		return nil
	}

	// Get the list of page numbers to copy
	pageNumbers := make([]uint32, 0, len(localCache))
	for pageNumber, entry := range localCache {
		if entry.SequenceNumber >= commitSequence {
			pageNumbers = append(pageNumbers, pageNumber)
		}
	}

	// Sort page numbers for sequential access (faster writes)
	sort.Slice(pageNumbers, func(i, j int) bool {
		return pageNumbers[i] < pageNumbers[j]
	})

	// Copy pages to the index file in sorted order
	for _, pageNumber := range pageNumbers {
		entry := localCache[pageNumber]
		if entry.SequenceNumber != commitSequence {
			continue // Skip if sequence number doesn't match
		}

		// Calculate the offset in the index file
		offset := int64(pageNumber) * PageSize

		// Write the page data to the index file
		if _, err := db.indexFile.WriteAt(entry.Data, offset); err != nil {
			return fmt.Errorf("failed to write page %d to index file: %w", pageNumber, err)
		}

		// Ensure the index file size is updated if necessary
		finalOffset := offset + PageSize
		if finalOffset > db.indexFileSize {
			db.indexFileSize = finalOffset
		}
	}

	return nil
}

// copyValuesToValuesFile copies values from the value cache to the values file
func (db *DB) copyValuesToValuesFile(commitSequence int64) error {
	var offsets []int64

	// Get all values with the specified commit sequence
	db.valueCacheMutex.RLock()
	for offset, entry := range db.valueCache {
		// Find the version with the specified commit sequence
		for ; entry != nil; entry = entry.next {
			if entry.txnSequence == commitSequence {
				offsets = append(offsets, offset)
				break
			}
		}
	}
	db.valueCacheMutex.RUnlock()

	// Sort offsets for sequential access
	sort.Slice(offsets, func(i, j int) bool {
		return offsets[i] < offsets[j]
	})

	// Copy values to the values file in sorted order
	for _, offset := range offsets {
		db.valueCacheMutex.RLock()
		entry := db.valueCache[offset]
		// Find the version with the specified commit sequence
		for ; entry != nil; entry = entry.next {
			if entry.txnSequence == commitSequence {
				break
			}
		}
		db.valueCacheMutex.RUnlock()

		if entry == nil {
			continue // Entry not found for this commit sequence
		}

		var valueData []byte
		if offset == FreeValuesListOffset {
			// use the raw value data
			valueData = entry.value
		} else {
			// Prepare the value data with type, size, and value
			valueData = prepareValueData(entry.value)
		}

		// Write the value data to the values file
		if _, err := db.valuesFile.WriteAt(valueData, offset); err != nil {
			return fmt.Errorf("failed to write value at offset %d to values file: %w", offset, err)
		}

		// Update values file size if necessary (only if we're extending the file)
		finalOffset := offset + int64(len(valueData))
		if finalOffset > db.valuesFileSize {
			db.valuesFileSize = finalOffset
		}
	}

	return nil
}

// copyWALContentToDbFiles copies pages from the WAL cache to the index file and values to the values file
func (db *DB) copyWALContentToDbFiles() error {
	if db.walInfo == nil {
		return nil
	}

	// Copy values to the values file
	if err := db.copyWALValuesToValuesFile(); err != nil {
		return fmt.Errorf("failed to copy values to values file: %w", err)
	}

	// Copy pages to the index file
	if err := db.copyWALPagesToIndexFile(); err != nil {
		return fmt.Errorf("failed to copy pages to index file: %w", err)
	}

	return nil
}

// copyWALPagesToIndexFile copies pages from the WAL cache to the index file
func (db *DB) copyWALPagesToIndexFile() error {

	// Copy pages
	var pageNumbers []uint32

	for bucketIdx := 0; bucketIdx < 1024; bucketIdx++ {
		bucket := &db.pageCache[bucketIdx]
		bucket.mutex.RLock()

		for pageNumber := range bucket.pages {
			pageNumbers = append(pageNumbers, pageNumber)
		}

		bucket.mutex.RUnlock()
	}

	// Sort page numbers for sequential access (faster writes)
	sort.Slice(pageNumbers, func(i, j int) bool {
		return pageNumbers[i] < pageNumbers[j]
	})

	// Copy pages to the index file in sorted order
	for _, pageNumber := range pageNumbers {
		// Get the head of the linked list for this page number
		bucket := &db.pageCache[pageNumber & 1023]
		bucket.mutex.Lock()

		headPage, exists := bucket.pages[pageNumber]
		if !exists {
			bucket.mutex.Unlock()
			continue
		}

		// Find the first WAL page in the linked list
		var walPage *Page = nil
		for page := headPage; page != nil; page = page.next {
			if page.isWAL {
				walPage = page
				break
			}
		}
		bucket.mutex.Unlock()

		// Skip if no WAL page was found
		if walPage == nil {
			continue
		}

		// Calculate the offset in the index file
		offset := int64(pageNumber) * PageSize

		// Write the page data to the index file
		if _, err := db.indexFile.WriteAt(walPage.data, offset); err != nil {
			return fmt.Errorf("failed to write page %d to index file: %w", pageNumber, err)
		}
	}

	return nil
}

// copyWALValuesToValuesFile copies values from the WAL cache to the values file
func (db *DB) copyWALValuesToValuesFile() error {
	var valueOffsets []int64

	// Copy values
	db.valueCacheMutex.RLock()
	for offset, entry := range db.valueCache {
		// Find the first WAL version
		for ; entry != nil; entry = entry.next {
			if entry.isWAL {
				valueOffsets = append(valueOffsets, offset)
				break
			}
		}
	}
	db.valueCacheMutex.RUnlock()

	// Sort value offsets for sequential access
	sort.Slice(valueOffsets, func(i, j int) bool {
		return valueOffsets[i] < valueOffsets[j]
	})

	// Copy values to the values file in sorted order
	for _, offset := range valueOffsets {
		db.valueCacheMutex.RLock()
		entry := db.valueCache[offset]
		// Find the first WAL version
		for ; entry != nil; entry = entry.next {
			if entry.isWAL {
				break
			}
		}
		db.valueCacheMutex.RUnlock()

		if entry == nil {
			continue // No WAL entry found
		}

		var valueData []byte
		if offset == FreeValuesListOffset {
			// use the raw value data
			valueData = entry.value
		} else {
			// Prepare the value data with type, size, and value
			valueData = prepareValueData(entry.value)
		}

		// Write the value data to the values file
		if _, err := db.valuesFile.WriteAt(valueData, offset); err != nil {
			return fmt.Errorf("failed to write value at offset %d to values file: %w", offset, err)
		}
	}

	return nil
}

// openWAL opens an existing WAL file without creating it
func (db *DB) openWAL() error {
	// If WAL file is already open, return
	if db.walInfo != nil {
		return nil
	}

	// Generate WAL file path by appending "-wal" to the main db file path
	walPath := db.filePath + "-wal"

	// Check if WAL file exists
	if _, err := os.Stat(walPath); err != nil {
		// WAL file doesn't exist, create it
		return db.createWAL()
	}

	// Open the existing WAL file
	walFile, err := os.OpenFile(walPath, os.O_RDWR, 0666)
	if err != nil {
		return fmt.Errorf("failed to open existing WAL file: %w", err)
	}

	// Initialize WAL info
	db.walInfo = &WalInfo{
		file:      walFile,
		walPath:   walPath,
		hasher:    crc32.IEEETable,
	}

	// Scan the WAL file to load any existing frames
	if err := db.scanWAL(); err != nil {
		// If there's an error scanning the WAL (which could be due to a database ID mismatch),
		// the scanWAL function will handle it by deleting and recreating the WAL file
		return err
	}

	return nil
}

func (db *DB) closeWAL() error {
	if db.walInfo == nil {
		return nil
	}

	// Close the WAL file
	if err := db.walInfo.file.Close(); err != nil {
		return fmt.Errorf("failed to close WAL file: %w", err)
	}

	// Reset the WAL info
	db.walInfo = nil

	return nil
}

// resetWAL resets the WAL file
func (db *DB) resetWAL() error {
	if db.walInfo == nil {
		return nil
	}

	// Truncate the WAL file
	if err := db.walInfo.file.Truncate(0); err != nil {
		return fmt.Errorf("failed to truncate WAL file: %w", err)
	}

	// Close the WAL file
	if err := db.walInfo.file.Close(); err != nil {
		return fmt.Errorf("failed to close WAL file: %w", err)
	}

	// Create a new WAL file
	return db.createWAL()
}

// deleteWAL deletes the WAL file associated with the database
func (db *DB) deleteWAL() error {
	// If the WAL info is not initialized, nothing to do
	if db.walInfo == nil {
		// Generate WAL file path by appending "-wal" to the main db file path
		walPath := db.filePath + "-wal"

		// Check if WAL file exists
		if _, err := os.Stat(walPath); err == nil {
			// WAL file exists, delete it
			if err := os.Remove(walPath); err != nil {
				return fmt.Errorf("failed to delete WAL file: %w", err)
			}
		}
		return nil
	}

	// Close the WAL file if it's open
	if db.walInfo.file != nil {
		if err := db.walInfo.file.Close(); err != nil {
			return fmt.Errorf("failed to close WAL file: %w", err)
		}
	}

	// Delete the WAL file
	if err := os.Remove(db.walInfo.walPath); err != nil {
		return fmt.Errorf("failed to delete WAL file: %w", err)
	}

	// Reset WAL info
	db.walInfo = nil

	return nil
}
