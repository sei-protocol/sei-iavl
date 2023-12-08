package main

import (
	"errors"
	"fmt"
	"github.com/cosmos/iavl"
	"github.com/cosmos/iavl/changeset"
	"github.com/spf13/cobra"
)

func PrintChangeSetCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "print",
		Short: "Replay the input change set files and print all iavl changeset",
		RunE:  handlePrintChangeset,
	}
	cmd.PersistentFlags().StringP("input-file", "f", "", "Full file path of the changeset file")
	return cmd
}

func handlePrintChangeset(cmd *cobra.Command, _ []string) error {
	inputFile, _ := cmd.Flags().GetString("input-file")
	if inputFile == "" {
		return errors.New("input-file is required")
	}
	importer := changeset.NewImporter(inputFile).WithProcessFn(func(version int64, cs *iavl.ChangeSet) (bool, error) {
		if cs != nil && version >= 0 {
			for _, pair := range cs.Pairs {
				fmt.Printf("Version: %d, delete: %t, key: %X, value: %X\n", version, pair.Delete, pair.Key, pair.Value)
			}
		}
		return false, nil
	})
	_, err := importer.Start()
	if err != nil {
		panic(err)
	}
	return nil
}
