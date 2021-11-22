package cmd

import (
	"github.com/GlobalFishingWatch/bigquery-tool/internal/action"
	"github.com/GlobalFishingWatch/bigquery-tool/types"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"log"
)

func init() {

	createTableCmd.Flags().StringP("project-id", "", "", "The destination project id")
	createTableCmd.MarkFlagRequired("project-id")

	createTableCmd.Flags().StringP("dataset-id", "", "", "The destination dataset")
	createTableCmd.MarkFlagRequired("dataset-id")

	createTableCmd.Flags().StringP("table-name", "", "", "The name of the destination table")
	createTableCmd.MarkFlagRequired("table-name")

	createTableCmd.Flags().StringP("query", "", "", "The query to execute")
	createTableCmd.MarkFlagRequired("query")



	viper.BindPFlag("create-table-query", createTableCmd.Flags().Lookup("query"))
	viper.BindPFlag("create-table-project-id", createTableCmd.Flags().Lookup("project-id"))
	viper.BindPFlag("create-table-dataset-id", createTableCmd.Flags().Lookup("dataset-id"))
	viper.BindPFlag("create-table-name", createTableCmd.Flags().Lookup("table-name"))

	rootCmd.AddCommand(createTableCmd)
}

var createTableCmd = &cobra.Command{
	Use:   "create-table",
	Short: "Execute raw sql",
	Long:  `Execute raw sql
Format:
	postgres create-table --project-id= --sql= 
Example:
	postgres create-table \
	  --project-id=world-fishing \
	  --sql="SELECT * FROM vessels;"
`,
	Run: func(cmd *cobra.Command, args []string) {
		log.Println("→ Executing create table command")

		params := types.ExecuteCreateTableParams{
			Query:        	viper.GetString("create-table-query"),
			ProjectId:  	viper.GetString("create-table-project-id"),
			TableName:  viper.GetString("create-table-name"),
			DatasetId:  viper.GetString("create-table-dataset-id"),
		}
		log.Println(params)

		action.ExecuteCreateTable(params)
		log.Println("→ Executing create table finished")
	},
}

