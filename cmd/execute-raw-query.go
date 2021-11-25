package cmd

import (
	"github.com/GlobalFishingWatch/bigquery-tool/internal/action"
	"github.com/GlobalFishingWatch/bigquery-tool/types"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"log"
)

func init() {

	executeRawQueryCmd.Flags().StringP("project-id", "", "", "The destination project id")
	executeRawQueryCmd.MarkFlagRequired("project-id")

	executeRawQueryCmd.Flags().StringP("query", "", "", "The query to execute")
	executeRawQueryCmd.MarkFlagRequired("query")

	viper.BindPFlag("execute-raw-query", executeRawQueryCmd.Flags().Lookup("query"))
	viper.BindPFlag("execute-raw-project-id", executeRawQueryCmd.Flags().Lookup("project-id"))

	rootCmd.AddCommand(executeRawQueryCmd)
}

var executeRawQueryCmd = &cobra.Command{
	Use:   "execute-raw-query",
	Short: "Execute raw sql",
	Long:  `Execute raw sql
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
			Query:     viper.GetString("execute-raw-query"),
			ProjectId: viper.GetString("execute-raw-project-id"),
		}
		log.Println(params)

		action.ExecuteRawQuery(params)
		log.Println("→ Executing raw query finished")
	},
}

