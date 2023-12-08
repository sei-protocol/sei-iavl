package changeset

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"math/bits"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/alitto/pond"
	"github.com/cosmos/iavl"
	"github.com/klauspost/compress/zstd"
	dbm "github.com/tendermint/tm-db"
)

const (
	DefaultCacheSize int = 10000000
)

type Exporter struct {
	db          *dbm.PrefixDB
	start       int64
	end         int64
	concurrency int
	segmentSize int64
	outputDir   string
}

func NewExporter(
	db *dbm.PrefixDB,
	start int64,
	end int64,
	concurrency int,
	segmentSize int64,
	outputDir string,
) *Exporter {
	return &Exporter{
		db:          db,
		start:       start,
		end:         end,
		concurrency: concurrency,
		segmentSize: segmentSize,
		outputDir:   outputDir,
	}
}

func (exporter *Exporter) Start() error {
	// use a worker pool with enough buffer to parallelize the export
	pool := pond.New(exporter.concurrency, 10240)
	defer pool.StopAndWait()

	// share the iavl tree between tasks to reuse the node cache
	iavlTreePool := sync.Pool{
		New: func() any {
			// use separate prefixdb and iavl tree in each task to maximize concurrency performance
			return iavl.NewImmutableTree(exporter.db, DefaultCacheSize, true)
		},
	}

	// split into segments
	var segmentSize = exporter.segmentSize
	for i := exporter.start; i < exporter.end; i += segmentSize {
		end := i + segmentSize
		if end > exporter.end {
			end = exporter.end
		}
		var chunkFiles []string
		group, _ := pool.GroupContext(context.Background())
		fmt.Printf("Start exporting segment %d-%d at %s\n", i, end, time.Now().Format(time.RFC3339))
		for _, workRange := range splitWorkLoad(exporter.concurrency, Range{i, end}) {
			workRange := workRange
			chunkFile := filepath.Join(exporter.outputDir, fmt.Sprintf("tmp-chunk-%d-%d.zst", workRange.Start, workRange.End))
			group.Submit(func() error {
				tree := iavlTreePool.Get().(*iavl.ImmutableTree)
				defer iavlTreePool.Put(tree)
				fmt.Printf("Start exporting chunk %s at %s\n", chunkFile, time.Now().Format(time.RFC3339))
				err := writeChangesetChunks(chunkFile, tree, workRange.Start, workRange.End)
				fmt.Printf("Finished exporting chunk %s at %s\n", chunkFile, time.Now().Format(time.RFC3339))
				return err
			})
			chunkFiles = append(chunkFiles, chunkFile)
		}

		if err := group.Wait(); err != nil {
			return err
		}
		segmentFile := filepath.Join(exporter.outputDir, fmt.Sprintf("changeset-%d-%d.zst", i, end))
		err := collectChunksToSegment(segmentFile, chunkFiles)
		if err != nil {
			return err
		}
		fmt.Printf("Finished exporting segment %d-%d at %s\n", i, end, time.Now().Format(time.RFC3339))
	}

	return nil
}

func writeChangesetChunks(chunkFilePath string, tree *iavl.ImmutableTree, start int64, end int64) (returnErr error) {
	fmt.Printf("Exporting changeset chunk %d-%d\n", start, end)
	chunkFile, err := createFile(chunkFilePath)
	zstdWriter, err := zstd.NewWriter(chunkFile)

	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return err
	}

	defer func() {
		err := zstdWriter.Close()
		if err != nil {
			returnErr = err
		}
		if err := chunkFile.Close(); returnErr == nil {
			returnErr = err
		}
	}()

	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return err
	}
	if err := tree.TraverseStateChanges(start, end, func(version int64, changeSet *iavl.ChangeSet) error {
		return WriteChangeSet(zstdWriter, version, *changeSet)
	}); err != nil {
		fmt.Printf("Error: %v\n", err)
		return err
	}

	return zstdWriter.Flush()
}

func collectChunksToSegment(outputFile string, chunkFiles []string) error {
	fp, err := createFile(outputFile)
	if err != nil {
		return err
	}

	bufWriter := bufio.NewWriter(fp)
	writer, _ := zstd.NewWriter(bufWriter)

	defer fp.Close()

	for _, chunkFile := range chunkFiles {
		if err := copyTmpFile(writer, chunkFile); err != nil {
			return err
		}
		if err := os.Remove(chunkFile); err != nil {
			return err
		}
	}

	// Write ending header
	var endingHeader [16]byte
	binary.LittleEndian.PutUint64(endingHeader[:], uint64(math.MaxUint64))
	binary.LittleEndian.PutUint64(endingHeader[8:16], uint64(0))
	if _, err := writer.Write(endingHeader[:]); err != nil {
		return err
	}

	err = writer.Close()
	if err != nil {
		return err
	}
	return bufWriter.Flush()
}

// copyTmpFile append the compressed temporary file to writer
func copyTmpFile(writer io.Writer, tmpFile string) error {
	fp, err := os.Open(tmpFile)
	if err != nil {
		return err
	}
	defer fp.Close()

	reader, _ := zstd.NewReader(fp)
	_, err = io.Copy(writer, reader)
	return err
}

func createFile(name string) (*os.File, error) {
	return os.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
}

func WriteChangeSet(writer io.Writer, version int64, cs iavl.ChangeSet) error {
	if len(cs.Pairs) <= 0 {
		return nil
	}

	var size int
	items := make([][]byte, 0, len(cs.Pairs))
	for _, pair := range cs.Pairs {
		buf, err := encodeKVPair(pair)
		if err != nil {
			return err
		}
		size += len(buf)
		items = append(items, buf)
	}

	// Write header
	fmt.Printf("Writing version %d with %d items at %s\n", version, len(items), time.Now().Format(time.RFC3339))
	var versionHeader [16]byte
	binary.LittleEndian.PutUint64(versionHeader[:], uint64(version))
	binary.LittleEndian.PutUint64(versionHeader[8:16], uint64(size))

	if _, err := writer.Write(versionHeader[:]); err != nil {
		return err
	}
	for _, item := range items {
		if _, err := writer.Write(item); err != nil {
			return err
		}
	}
	return nil
}

type Range struct {
	Start, End int64
}

func splitWorkLoad(workers int, full Range) []Range {
	var ranges []Range
	chunkSize := (full.End - full.Start + int64(workers) - 1) / int64(workers)
	for i := full.Start; i < full.End; i += chunkSize {
		end := i + chunkSize
		if end > full.End {
			end = full.End
		}
		ranges = append(ranges, Range{Start: i, End: end})
	}
	return ranges
}

// encodeKVPair encode a key-value pair in change set.
// see godoc of `encodedSizeOfKVPair` for layout description,
// returns error if key/value length overflows.
func encodeKVPair(pair *iavl.KVPair) ([]byte, error) {
	buf := make([]byte, encodedSizeOfKVPair(pair))

	offset := 1
	keyLen := len(pair.Key)

	written := binary.PutUvarint(buf[offset:], uint64(keyLen))
	offset += written

	copy(buf[offset:], pair.Key)
	if pair.Delete {
		buf[0] = 1
		return buf, nil
	} else {
		buf[0] = 0
	}

	offset += keyLen

	valueLen := len(pair.Value)
	written = binary.PutUvarint(buf[offset:], uint64(valueLen))
	offset += written

	copy(buf[offset:], pair.Value)

	return buf, nil
}

// encodedSizeOfKVPair returns the encoded length of a key-value pair
//
// layout: deletion(1) + keyLen(varint) + key + [ valueLen(varint) + value ]
func encodedSizeOfKVPair(pair *iavl.KVPair) int {
	keyLen := len(pair.Key)
	size := 1 + uvarintSize(uint64(keyLen)) + keyLen
	if pair.Delete {
		return size
	}

	valueLen := len(pair.Value)
	return size + uvarintSize(uint64(valueLen)) + valueLen
}

// uvarintSize function calculates the size (in bytes) needed to encode an unsigned integer in a variable-length format
// based on the number of bits required to represent the integer's value.
// This is a common operation when serializing data structures into binary formats where compactness and
// variable-length encoding are desired.
func uvarintSize(num uint64) int {
	bits := bits.Len64(num)
	q, r := bits/7, bits%7
	size := q
	if r > 0 || size == 0 {
		size++
	}
	return size
}
