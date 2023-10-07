package changeset

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"math/bits"
	"os"
	"path/filepath"
	"sync"

	"github.com/alitto/pond"
	"github.com/cosmos/iavl"
	"github.com/klauspost/compress/zstd"
	dbm "github.com/tendermint/tm-db"
)

const (
	DefaultCacheSize int = 1000000
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
	pool := pond.New(exporter.concurrency, 1024)
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
	var groups []*pond.TaskGroupWithContext
	for i := exporter.start; i < exporter.end; i += segmentSize {
		end := i + segmentSize
		if end > exporter.end {
			end = exporter.end
		}
		group, _ := pool.GroupContext(context.Background())
		startPos := i
		endPos := end
		group.Submit(func() error {
			tree := iavlTreePool.Get().(*iavl.ImmutableTree)
			defer iavlTreePool.Put(tree)
			err := dumpChangesetSegment(exporter.outputDir, tree, startPos, endPos)
			fmt.Printf("Finished exporting segment %d-%d\n", startPos, endPos)
			return err
		})
		groups = append(groups, group)
	}

	// wait for all task groups to complete
	for _, group := range groups {
		if err := group.Wait(); err != nil {
			fmt.Printf("Error: %v\n", err)
			return err
		}
	}
	return nil
}

func dumpChangesetSegment(outputDir string, tree *iavl.ImmutableTree, start int64, end int64) (returnErr error) {
	fmt.Printf("Exporting changeset segment %d-%d\n", start, end)
	segmentFilePath := filepath.Join(outputDir, fmt.Sprintf("changeset-%d-%d.zst", start, end))
	segmentFile, err := createFile(segmentFilePath)
	zstdWriter, err := zstd.NewWriter(segmentFile)

	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return err
	}

	defer func() {
		err := zstdWriter.Close()
		if err != nil {
			returnErr = err
		}
		if err := segmentFile.Close(); returnErr == nil {
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

func createFile(name string) (*os.File, error) {
	return os.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
}

func WriteChangeSet(writer io.Writer, version int64, cs iavl.ChangeSet) error {
	if len(cs.Pairs) <= 0 {
		return nil
	}

	fmt.Printf("Version: %d\n", version)
	var size int
	items := make([][]byte, 0, len(cs.Pairs))
	for _, pair := range cs.Pairs {
		buf, err := encodeKVPair(pair)
		if err != nil {
			fmt.Printf("Error: %v\n", err)
			return err
		}
		size += len(buf)
		items = append(items, buf)
	}

	// Write header
	var versionHeader [16]byte
	binary.LittleEndian.PutUint64(versionHeader[:], uint64(version))
	binary.LittleEndian.PutUint64(versionHeader[8:], uint64(size))

	fmt.Printf("Total Size: %d\n", size)

	if _, err := writer.Write(versionHeader[:]); err != nil {
		fmt.Printf("Error: %v\n", err)
		return err
	}
	for _, item := range items {
		if _, err := writer.Write(item); err != nil {
			fmt.Printf("Error: %v\n", err)
			return err
		}
	}
	return nil
}

// encodeKVPair encode a key-value pair in change set.
// see godoc of `encodedSizeOfKVPair` for layout description,
// returns error if key/value length overflows.
func encodeKVPair(pair *iavl.KVPair) ([]byte, error) {
	buf := make([]byte, encodedSizeOfKVPair(pair))
	fmt.Printf("encodedSizeOfKVPair: %d\n", encodedSizeOfKVPair(pair))

	offset := 1
	keyLen := len(pair.Key)
	fmt.Printf("keyLen: %d, offset: %d\n", keyLen, offset)

	written := binary.PutUvarint(buf[offset:], uint64(keyLen))
	offset += written
	fmt.Printf("offset: %d\n", written, offset)

	copy(buf[offset:], pair.Key)
	if pair.Delete {
		buf[0] = 1
		return buf, nil
	} else {
		buf[0] = 0
	}

	offset += keyLen
	fmt.Printf("offset: %d\n", offset)

	valueLen := len(pair.Value)
	offset += binary.PutUvarint(buf[offset:], uint64(valueLen))
	copy(buf[offset:], pair.Value)

	fmt.Printf("valueLen: %d, offset: %d\n", valueLen, offset)

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