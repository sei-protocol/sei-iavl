package main

import (
	"fmt"
	"github.com/cosmos/iavl"
	"github.com/spf13/cobra"
	dbm "github.com/tendermint/tm-db"
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
	rootCmd.PersistentFlags().String("home-dir", "/root/.sei/data/application.db", "Home directory")
	rootCmd.PersistentFlags().Int64("start-version", 0, "start version")
	rootCmd.PersistentFlags().Int64("end-version", 0, "start version")
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
			fmt.Fprintf(os.Stderr, "Error hashing tree: %s\n", err)
			os.Exit(1)
		}
		fmt.Printf("Tree hash is %X, tree size is %d\n", treeHash, tree.ImmutableTree().Size())
		fmt.Printf("Going to traverse changeset from version %d to %d\n", startVersion, endVersion)
		if err := iavlTree.TraverseStateChanges(startVersion, endVersion, func(version int64, changeSet *iavl.ChangeSet) error {
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

func WriteChangeSet(version int64, cs iavl.ChangeSet) error {
	fmt.Printf("Process changeset for version:%d, %d pairs\n", version, len(cs.Pairs))
	for _, pair := range cs.Pairs {
		fmt.Printf("Version: %d, delete: %t, key: %X, value: %X\n", version, pair.Delete, pair.Key, pair.Value)
	}
	return nil
}
