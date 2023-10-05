package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/cosmos/iavl/cmd/changeset/changeset"
	"github.com/spf13/cobra"
	dbm "github.com/tendermint/tm-db"
)

func main() {
	rootCmd := &cobra.Command{
		Use:   "changeset",
		Short: "Tools for export/import IAVL changesets",
	}
	rootCmd.PersistentFlags().StringP("input-dir", "i", "/root/.sei/data/application.db", "Home directory")
	rootCmd.PersistentFlags().StringP("stores", "s", "", "comma separated store key names, such as bank,staking")
	rootCmd.PersistentFlags().StringP("output-dir", "o", "changeset-output", "output directory")

	rootCmd.AddCommand(ExportChangeSetCmd())
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func ExportChangeSetCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "export",
		Short: "Extract changesets from IAVL versions, and save to plain file format",
		RunE:  exportChangeset,
	}
	cmd.PersistentFlags().Int64("start-version", 0, "start version number")
	cmd.PersistentFlags().Int64("end-version", 0, "end version number")
	cmd.PersistentFlags().Int("concurrency", 10, "number of concurrent workers")
	cmd.PersistentFlags().Int64("segment-size", 1000000, "number of blocks in each segment")

	// we handle one stores at a time, because stores don't share much in db, handle concurrently reduces cache efficiency.
	return cmd
}

func exportChangeset(cmd *cobra.Command, _ []string) error {
	inputDir, _ := cmd.Flags().GetString("input-dir")
	storeKeys, _ := cmd.Flags().GetString("stores")
	startVersion, _ := cmd.Flags().GetInt64("start-version")
	endVersion, _ := cmd.Flags().GetInt64("end-version")
	concurrency, _ := cmd.Flags().GetInt("concurrency")
	segmentSize, _ := cmd.Flags().GetInt64("segment-size")
	outDir, _ := cmd.Flags().GetString("output-dir")

	if storeKeys == "" {
		return errors.New("stores is required")
	}
	storeKeysList := strings.Split(storeKeys, ",")
	db, err := OpenDB(inputDir)
	for _, storeKey := range storeKeysList {
		prefix := []byte(fmt.Sprintf("s/k:%s/", storeKey))
		fmt.Printf("Begin exporting store with prefix %s at %s\n", prefix, time.Now().Format(time.RFC3339))
		if err := os.MkdirAll(outDir, os.ModePerm); err != nil {
			return err
		}
		storeDir := filepath.Join(outDir, storeKey)
		if err := os.MkdirAll(storeDir, os.ModePerm); err != nil {
			return err
		}
		if err != nil {
			return err
		}
		prefixDB := dbm.NewPrefixDB(db, prefix)
		exporter := changeset.NewCSExporter(prefixDB, startVersion, endVersion, concurrency, segmentSize, storeDir)
		err = exporter.Export()
		if err != nil {
			return err
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
