package cmd

import (
	"github.com/spf13/cobra"
	"log"
	"os"
)

var cfgFile string

var rootCmd = &cobra.Command{
	Use:   "bigquery-tool",
	Short: "A CLI to manage bigquery commands",
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		log.Println(err)
		os.Exit(1)
	}
}

