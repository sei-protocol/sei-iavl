package main

import (
	"fmt"
	"github.com/cosmos/iavl"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	dbm "github.com/tendermint/tm-db"
	"math"
	"os"
	"strings"
	"time"
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
	rootCmd.PersistentFlags().String("home-dir", "/root/.sei", "Home directory")
	rootCmd.PersistentFlags().String("start-version", "0", "start version")
	rootCmd.PersistentFlags().String("end-version", "0", "start version")
	rootCmd.PersistentFlags().StringP("store", "s", DefaultStore, "store key name")
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
	prefix := []byte(storeKey)
	if storeKey != "" {
		fmt.Println("Begin dumping store", storeKey, time.Now().Format(time.RFC3339))
		db, err := OpenDB(homeDir)
		if err != nil {
			return nil
		}
		if len(storeKey) != 0 {
			db = dbm.NewPrefixDB(db, prefix)
		}
		tree, err := iavl.NewMutableTree(db, DefaultCacheSize, true)
		if err != nil {
			return err
		}
		// Make sure we have a correct end version
		if endVersion <= 0 {
			latestVersion, _ := tree.LazyLoadVersion(0)
			endVersion = latestVersion + 1
		}

		storeStartVersion, err := getNextVersion(dbm.NewPrefixDB(db, prefix), 0)
		if storeStartVersion <= 0 {
			// store not exists
			return errors.New("skip empty store")
		}
		if startVersion > storeStartVersion {
			storeStartVersion = startVersion
		}
		iavlTree := iavl.NewImmutableTree(dbm.NewPrefixDB(db, prefix), DefaultCacheSize, true)
		if err := iavlTree.TraverseStateChanges(storeStartVersion, endVersion, func(version int64, changeSet *iavl.ChangeSet) error {
			return WriteChangeSet(version, *changeSet)
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

func getNextVersion(db dbm.DB, version int64) (int64, error) {
	rootKeyFormat := iavl.NewKeyFormat('r', 8)
	itr, err := db.Iterator(
		rootKeyFormat.Key(version+1),
		rootKeyFormat.Key(math.MaxInt64),
	)
	if err != nil {
		return 0, err
	}
	defer itr.Close()

	var nversion int64
	for ; itr.Valid(); itr.Next() {
		rootKeyFormat.Scan(itr.Key(), &nversion)
		return nversion, nil
	}

	if err := itr.Error(); err != nil {
		return 0, err
	}

	return 0, nil
}

func WriteChangeSet(version int64, cs iavl.ChangeSet) error {
	for _, pair := range cs.Pairs {
		fmt.Printf("Version: %d, delete: %t, key: %X, value: %X\n", version, pair.Delete, pair.Key, pair.Key)
	}
	return nil
}
