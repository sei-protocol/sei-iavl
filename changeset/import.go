package changeset

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"github.com/DataDog/zstd"
	"github.com/cosmos/iavl"
	"io"
	"os"
)

type Importer struct {
	inputFile string
	processFn func(version int64, changeset *iavl.ChangeSet) (bool, error)
}

func NewImporter(inputFile string) *Importer {
	return &Importer{
		inputFile: inputFile,
	}
}

func (importer *Importer) WithProcessFn(processFn func(version int64, changeset *iavl.ChangeSet) (bool, error)) *Importer {
	importer.processFn = processFn
	return importer
}

// Reader combines `io.Reader` and `io.ByteReader`.
type Reader interface {
	io.ReadCloser
	io.ByteReader
	io.Closer
}

func (importer *Importer) Start() (int64, error) {
	reader, err := openChangesetFile(importer.inputFile)
	if err != nil {
		return 0, err
	}
	defer func(reader Reader) {
		_ = reader.Close()
	}(reader)
	return iterateChangeSet(reader, importer.processFn)

}

func openChangesetFile(fileName string) (Reader, error) {
	fp, err := os.Open(fileName)
	if err != nil {
		return nil, err
	}
	zstdReaderCloser := zstd.NewReader(fp)
	bufReader := bufio.NewReader(zstdReaderCloser)
	return &WrapReader{zstdReaderCloser, bufReader, fp}, nil
}

func iterateChangeSet(reader Reader, fn func(version int64, changeset *iavl.ChangeSet) (bool, error)) (int64, error) {
	var lastOffset int64
	for true {
		offset, version, changeSet, err := readNextChangeset(reader)
		if err != nil {
			if err == io.EOF {
				// Reaching here means we are done reading everything in the file
				break
			} else {
				return lastOffset, err
			}
		}
		shouldStop, err := fn(version, changeSet)
		lastOffset += offset
		if shouldStop {
			break
		}
	}
	return lastOffset, nil
}

// readNextChangeset reads the next changeset from the given reader and returns
// the read offset, the version, the changeset itself and the error
func readNextChangeset(reader Reader) (int64, int64, *iavl.ChangeSet, error) {
	var versionHeader [16]byte
	if _, err := io.ReadFull(reader, versionHeader[:]); err != nil {
		return 0, 0, nil, err
	}
	// Read header
	version := binary.LittleEndian.Uint64(versionHeader[:8])
	size := int64(binary.LittleEndian.Uint64(versionHeader[8:16]))
	if size <= 0 {
		return 16, int64(version), nil, nil
	}
	var changeset iavl.ChangeSet
	var offset int64
	for offset < size {
		pair, err := readKVPair(reader)
		if err != nil {
			return 0, 0, nil, err
		}
		offset += int64(encodedSizeOfKVPair(pair))
		changeset.Pairs = append(changeset.Pairs, pair)
	}
	if offset != size {
		return 0, 0, nil, fmt.Errorf("read version %d beyond payload size limit, size: %d, offset: %d", version, size, offset)
	}
	return size + 16, int64(version), &changeset, nil
}

// readKVPair decode a key-value pair from reader
// see `encodedSizeOfKVPair` for layout description
func readKVPair(reader Reader) (*iavl.KVPair, error) {
	deletion, err := reader.ReadByte()
	if err != nil {
		return nil, err
	}

	keyLen, err := binary.ReadUvarint(reader)
	if err != nil {
		return nil, err
	}

	pair := iavl.KVPair{
		Delete: deletion == 1,
		Key:    make([]byte, keyLen),
	}
	if _, err := io.ReadFull(reader, pair.Key); err != nil {
		return nil, err
	}

	if pair.Delete {
		return &pair, nil
	}

	valueLen, err := binary.ReadUvarint(reader)
	if err != nil {
		return nil, err
	}
	pair.Value = make([]byte, valueLen)
	if _, err := io.ReadFull(reader, pair.Value); err != nil {
		return nil, err
	}

	return &pair, nil
}
