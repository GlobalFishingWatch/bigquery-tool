package cmd

import (
	"log"

	"github.com/GlobalFishingWatch/bigquery-tool/internal/action"
	"github.com/GlobalFishingWatch/bigquery-tool/types"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func init() {

	executeRawQueryCmd.Flags().StringP("project-id", "", "", "The destination project id")
	executeRawQueryCmd.MarkFlagRequired("project-id")

	executeRawQueryCmd.Flags().StringP("query", "", "", "The query to execute")
	executeRawQueryCmd.MarkFlagRequired("query")

	executeRawQueryCmd.Flags().StringP("destination-dataset", "", "", "The destination dataset")

	executeRawQueryCmd.Flags().StringP("destination-table", "", "", "The destination table")

	executeRawQueryCmd.Flags().StringP("write-disposition", "", "WRITE_APPEND", "Specifies how existing data in the destination table is treated. Possible value (WRITE_EMPTY, WRITE_TRUNCATE, WRITE_APPEND)")

	viper.BindPFlag("execute-raw-query", executeRawQueryCmd.Flags().Lookup("query"))
	viper.BindPFlag("execute-raw-project-id", executeRawQueryCmd.Flags().Lookup("project-id"))
	viper.BindPFlag("execute-raw-destination-table", executeRawQueryCmd.Flags().Lookup("destination-table"))
	viper.BindPFlag("execute-raw-destination-dataset", executeRawQueryCmd.Flags().Lookup("destination-dataset"))
	viper.BindPFlag("execute-raw-write-disposition", executeRawQueryCmd.Flags().Lookup("write-disposition"))

	rootCmd.AddCommand(executeRawQueryCmd)
}

var executeRawQueryCmd = &cobra.Command{
	Use:   "execute-raw-query",
	Short: "Execute raw sql",
	Long: `Execute raw sql
Format:
	bigquery execute-raw-query --project-id= --sql= 
Example:
	bigquery execute-raw-query \
	  --project-id=world-fishing \
	  --sql="SELECT * FROM vessels;"
`,
	Run: func(cmd *cobra.Command, args []string) {
		log.Println("→ Executing raw query command")

		params := types.ExecuteRawQueryParams{
			Query:              viper.GetString("execute-raw-query"),
			ProjectId:          viper.GetString("execute-raw-project-id"),
			DestinationTable:   viper.GetString("execute-raw-destination-table"),
			DestinationDataset: viper.GetString("execute-raw-destination-dataset"),
			WriteDisposition:   viper.GetString("execute-raw-write-disposition"),
		}
		log.Println(params)

		action.ExecuteRawQuery(params)
		log.Println("→ Executing raw query finished")
	},
}
