package main

import (
	"encoding/binary"
	"fmt"
	"github.com/cosmos/iavl"
	"github.com/spf13/cobra"
	dbm "github.com/tendermint/tm-db"
	"io"
	"math/bits"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/klauspost/compress/zstd"
)

const (
	DefaultCacheSize int    = 10000
	DefaultStore     string = ""
)

func main() {
	rootCmd := &cobra.Command{
		Use:   "changeset",
		Short: "Dump change sets files which can be ingested into DB",
	}
	rootCmd.PersistentFlags().StringP("home-dir", "h", "/root/.sei/data/application.db", "Home directory")
	rootCmd.PersistentFlags().Int64("start-version", 0, "start version")
	rootCmd.PersistentFlags().Int64("end-version", 0, "start version")
	rootCmd.PersistentFlags().StringP("store", "s", DefaultStore, "store key name")
	rootCmd.PersistentFlags().StringP("output-dir", "o", "output", "output directory")

	rootCmd.AddCommand(DumpChangeSetCmd())
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func DumpChangeSetCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "dump",
		Short: "Extract changesets from iavl versions, and save to plain file format",
		RunE:  executeDumpChangesetCmd,
	}

	// we handle one stores at a time, because stores don't share much in db, handle concurrently reduces cache efficiency.
	return cmd
}

func executeDumpChangesetCmd(cmd *cobra.Command, _ []string) error {
	homeDir, _ := cmd.Flags().GetString("home-dir")
	storeKey, _ := cmd.Flags().GetString("store")
	startVersion, _ := cmd.Flags().GetInt64("start-version")
	endVersion, _ := cmd.Flags().GetInt64("end-version")
	outDir, _ := cmd.Flags().GetString("output-dir")
	prefix := []byte(fmt.Sprintf("s/k:%s/", storeKey))

	if storeKey != "" {
		fmt.Println("Begin dumping store with prefix", prefix, time.Now().Format(time.RFC3339))
		if err := os.MkdirAll(outDir, os.ModePerm); err != nil {
			return err
		}
		db, err := OpenDB(homeDir)
		if err != nil {
			return err
		}
		tree, err := iavl.NewMutableTree(dbm.NewPrefixDB(db, prefix), DefaultCacheSize, true)
		if err != nil {
			return err
		}
		// Make sure we have a correct end version
		if endVersion <= 0 {
			latestVersion, _ := tree.LazyLoadVersion(0)
			fmt.Printf("Got tree version: %d\n", latestVersion)
			endVersion = latestVersion + 1
		}

		iavlTree := iavl.NewImmutableTree(dbm.NewPrefixDB(db, prefix), DefaultCacheSize, true)
		treeHash, err := iavlTree.Hash()
		if err != nil {
			return err
		}
		fmt.Printf("Tree hash is %X, tree size is %d\n", treeHash, tree.ImmutableTree().Size())
		fmt.Printf("Going to traverse changeset from version %d to %d\n", startVersion, endVersion)
		taskFile := filepath.Join(outDir, fmt.Sprintf("changeset-%s-%d.zst", storeKey, startVersion))
		outputFile, err := createFile(taskFile)
		if err != nil {
			return err
		}
		zstdWriter, err := zstd.NewWriter(outputFile)
		if err != nil {
			return err
		}
		if err := iavlTree.TraverseStateChanges(startVersion, endVersion, func(version int64, changeSet *iavl.ChangeSet) error {
			return WriteChangeSet(zstdWriter, version, *changeSet)
		}); err != nil {
			panic(err)
		}
	}
	return nil
}

func OpenDB(dir string) (dbm.DB, error) {
	switch {
	case strings.HasSuffix(dir, ".db"):
		dir = dir[:len(dir)-3]
	case strings.HasSuffix(dir, ".db/"):
		dir = dir[:len(dir)-4]
	default:
		return nil, fmt.Errorf("database directory must end with .db")
	}
	cut := strings.LastIndex(dir, "/")
	if cut == -1 {
		return nil, fmt.Errorf("cannot cut paths on %s", dir)
	}
	name := dir[cut+1:]
	db, err := dbm.NewGoLevelDB(name, dir[:cut])
	if err != nil {
		return nil, err
	}
	return db, nil
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
		fmt.Printf("Version: %d, delete: %t, key: %X, value: %X\n", version, pair.Delete, pair.Key, pair.Value)
		buf, err := encodeKVPair(pair)
		if err != nil {
			return err
		}
		size += len(buf)
		items = append(items, buf)
	}

	// Write header
	var versionHeader [16]byte
	binary.LittleEndian.PutUint64(versionHeader[:], uint64(version))
	binary.LittleEndian.PutUint64(versionHeader[8:], uint64(size))

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

// encodeKVPair encode a key-value pair in change set.
// see godoc of `encodedSizeOfKVPair` for layout description,
// returns error if key/value length overflows.
func encodeKVPair(pair *iavl.KVPair) ([]byte, error) {
	buf := make([]byte, encodedSizeOfKVPair(pair))

	offset := 1
	keyLen := len(pair.Key)
	offset += binary.PutUvarint(buf[offset:], uint64(keyLen))

	copy(buf[offset:], pair.Key)
	if pair.Delete {
		buf[0] = 1
		return buf, nil
	}

	offset += keyLen
	offset += binary.PutUvarint(buf[offset:], uint64(len(pair.Value)))
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
