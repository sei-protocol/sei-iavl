package main

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	dbm "github.com/tendermint/tm-db"

	"github.com/cosmos/gorocksdb"
	"github.com/cosmos/iavl"
	ibytes "github.com/cosmos/iavl/internal/bytes"
)

// TODO: make this configurable?
const (
	DefaultCacheSize int = 10000
)

type KeyValuePair struct {
	Key   []byte `json:"key"`
	Value []byte `json:"value"`
}

func main() {
	args := os.Args[1:]
	if len(args) < 3 || (args[0] != "data" && args[0] != "keys" && args[0] != "shape" && args[0] != "versions" && args[0] != "size" && args[0] != "kvEntries") {
		fmt.Fprintln(os.Stderr, "Usage: iaviewer <data|keys|shape|versions|size> <leveldb dir> <prefix> [version number]")
		fmt.Fprintln(os.Stderr, "<prefix> is the prefix of db, and the iavl tree of different modules in cosmos-sdk uses ")
		fmt.Fprintln(os.Stderr, "different <prefix> to identify, just like \"s/k:gov/\" represents the prefix of gov module")
		os.Exit(1)
	}

	version := 0
	if len(args) == 4 {
		var err error
		version, err = strconv.Atoi(args[3])
		if err != nil {
			fmt.Fprintf(os.Stderr, "Invalid version number: %s\n", err)
			os.Exit(1)
		}
	}

	tree, err := ReadTree(args[1], version, []byte(args[2]))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error reading data: %s\n", err)
		os.Exit(1)
	}
	_, err = tree.Hash()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error hashing tree: %s\n", err)
		os.Exit(1)
	}

	switch args[0] {
	case "data":
		PrintTreeData(tree)
	case "keys":
		PrintTreeData(tree)
	case "kvEntries":
		PrintTreeData(tree)
	case "shape":
		PrintShape(tree)
	case "versions":
		PrintVersions(tree)
	case "size":
		PrintSize(tree)
	}
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
	// TODO: doesn't work on windows!
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

func writeToRocksDBConcurrently(db *gorocksdb.DB, kvEntries []KeyValuePair, concurrency int, maxRetries int) {
	wg := &sync.WaitGroup{}
	chunks := len(kvEntries) / concurrency
	for i := 0; i < concurrency; i++ {
		start := i * chunks
		end := start + chunks
		if i == concurrency-1 {
			end = len(kvEntries)
		}
		wg.Add(1)
		go func(start, end int) {
			defer wg.Done()
			wo := gorocksdb.NewDefaultWriteOptions()
			for j := start; j < end; j++ {
				retries := 0
				for {
					if err := db.Put(wo, kvEntries[j].Key, kvEntries[j].Value); err != nil {
						retries++
						if retries > maxRetries {
							log.Printf("Failed to write key after %d attempts: %v", maxRetries, err)
							break
						}
						// TODO: Add a sleep or back-off before retrying
						// time.Sleep(time.Second * time.Duration(retries))
					} else {
						// Success, so break the retry loop
						break
					}
				}
			}
		}(start, end)
	}
	wg.Wait()
}

func RocksDBBenchmark(dataFile string, dbPath string) {
	opts := gorocksdb.NewDefaultOptions()
	opts.SetCreateIfMissing(true)

	db, err := gorocksdb.OpenDb(opts, dbPath)
	if err != nil {
		log.Fatalf("Failed to open the DB: %v", err)
	}
	defer db.Close()

	// Read key-value entries from the file
	kvEntries, err := ReadKVEntriesFromFile(dataFile)
	if err != nil {
		log.Fatalf("Failed to read KV entries: %v", err)
	}

	// Shuffle the entries
	RandomShuffle(kvEntries)

	// Define concurrency level and maximum retry attempts
	concurrency := 4
	maxRetries := 3

	// Write shuffled entries to RocksDB concurrently
	writeToRocksDBConcurrently(db, kvEntries, concurrency, maxRetries)
}

func RandomShuffle(kvPairs []KeyValuePair) {
	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(kvPairs), func(i, j int) {
		kvPairs[i], kvPairs[j] = kvPairs[j], kvPairs[i]
	})
}

func addRandomBytes(data []byte, numBytes int) []byte {
	randomBytes := make([]byte, numBytes)
	if _, err := rand.Read(randomBytes); err != nil {
		log.Fatalf("Failed to generate random bytes: %v", err)
	}
	return append(data, randomBytes...)
}

// nolint: deadcode
func PrintDBStats(db dbm.DB) {
	count := 0
	prefix := map[string]int{}
	itr, err := db.Iterator(nil, nil)
	if err != nil {
		panic(err)
	}

	defer itr.Close()
	for ; itr.Valid(); itr.Next() {
		key := ibytes.UnsafeBytesToStr(itr.Key()[:1])
		prefix[key]++
		count++
	}
	if err := itr.Error(); err != nil {
		panic(err)
	}
	fmt.Printf("DB contains %d entries\n", count)
	for k, v := range prefix {
		fmt.Printf("  %s: %d\n", k, v)
	}
}

// ReadTree loads an iavl tree from the directory
// If version is 0, load latest, otherwise, load named version
// The prefix represents which iavl tree you want to read. The iaviwer will always set a prefix.
func ReadTree(dir string, version int, prefix []byte) (*iavl.MutableTree, error) {
	db, err := OpenDB(dir)
	if err != nil {
		return nil, err
	}
	if len(prefix) != 0 {
		db = dbm.NewPrefixDB(db, prefix)
	}

	tree, err := iavl.NewMutableTree(db, DefaultCacheSize, true)
	if err != nil {
		return nil, err
	}
	_, err = tree.LoadVersion(int64(version))
	return tree, err
}

func PrintTreeData(tree *iavl.MutableTree) {
	tree.Iterate(func(key []byte, value []byte) bool {
		if err := writeByteSliceToStdout(key); err != nil {
			panic(err)
		}
		if err := writeByteSliceToStdout(value); err != nil {
			panic(err)
		}
		return false
	})
}

func ReadKVEntriesFromFile(filename string) ([]KeyValuePair, error) {
	f, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var kvPairs []KeyValuePair
	for {
		key, err := readByteSlice(f)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		value, err := readByteSlice(f)
		if err != nil {
			return nil, err
		}

		kvPairs = append(kvPairs, KeyValuePair{Key: key, Value: value})
	}

	fmt.Printf("kv Entries %+v\n", kvPairs)

	return kvPairs, nil
}

func readByteSlice(r io.Reader) ([]byte, error) {
	var length uint32
	if err := binary.Read(r, binary.LittleEndian, &length); err != nil {
		return nil, err
	}

	data := make([]byte, length)
	_, err := io.ReadFull(r, data)
	return data, err
}

func writeByteSliceToStdout(data []byte) error {
	length := uint32(len(data))
	if err := binary.Write(os.Stdout, binary.LittleEndian, length); err != nil {
		return err
	}
	_, err := os.Stdout.Write(data)
	return err
}

// parseWeaveKey assumes a separating : where all in front should be ascii,
// and all afterwards may be ascii or binary
func parseWeaveKey(key []byte) string {
	return encodeID(key)
}

// casts to a string if it is printable ascii, hex-encodes otherwise
func encodeID(id []byte) string {
	for _, b := range id {
		if b < 0x20 || b >= 0x80 {
			return strings.ToUpper(hex.EncodeToString(id))
		}
	}
	return string(id)
}

func PrintShape(tree *iavl.MutableTree) {
	// shape := tree.RenderShape("  ", nil)
	//TODO: handle this error
	shape, _ := tree.ImmutableTree().RenderShape("  ", nodeEncoder)
	fmt.Println(strings.Join(shape, "\n"))
}

func nodeEncoder(id []byte, depth int, isLeaf bool) string {
	prefix := fmt.Sprintf("-%d ", depth)
	if isLeaf {
		prefix = fmt.Sprintf("*%d ", depth)
	}
	if len(id) == 0 {
		return fmt.Sprintf("%s<nil>", prefix)
	}
	return fmt.Sprintf("%s%s", prefix, parseWeaveKey(id))
}

func PrintVersions(tree *iavl.MutableTree) {
	versions := tree.AvailableVersions()
	fmt.Println("Available versions:")
	for _, v := range versions {
		fmt.Printf("  %d\n", v)
	}
}

func PrintSize(tree *iavl.MutableTree) {
	count, totalKeySize, totalValueSize := 0, 0, 0
	keySizeByPrefix, valSizeByPrefix := map[byte]int{}, map[byte]int{}
	tree.Iterate(func(key []byte, value []byte) bool {
		count += 1
		totalKeySize += len(key)
		totalValueSize += len(value)
		if _, ok := keySizeByPrefix[key[0]]; !ok {
			keySizeByPrefix[key[0]] = 0
			valSizeByPrefix[key[0]] = 0
		}
		keySizeByPrefix[key[0]] += len(key)
		valSizeByPrefix[key[0]] += len(value)
		return false
	})
	fmt.Printf("Total entry count: %d. Total key bytes: %d. Total value bytes: %d\n", count, totalKeySize, totalValueSize)
	for p := range keySizeByPrefix {
		fmt.Printf("prefix %d has key bytes %d and value bytes %d\n", p, keySizeByPrefix[p], valSizeByPrefix[p])
	}
}
