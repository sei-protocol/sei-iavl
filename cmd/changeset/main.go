package main

import (
	"fmt"
	"github.com/spf13/cobra"
	"os"
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
	rootCmd.AddCommand()
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
