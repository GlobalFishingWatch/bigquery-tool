package cmd

import (
	"github.com/GlobalFishingWatch/bigquery-tool/internal/action"
	"github.com/GlobalFishingWatch/bigquery-tool/types"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"log"
)

func init() {

	createTemporalTableCmd.Flags().StringP("project-id", "", "", "The destination project id")
	createTemporalTableCmd.MarkFlagRequired("project-id")

	createTemporalTableCmd.Flags().StringP("temp-dataset-id", "", "", "The destination dataset")
	createTemporalTableCmd.MarkFlagRequired("temp-dataset-id")

	createTemporalTableCmd.Flags().StringP("temp-table-name", "", "", "The name of the destination table")
	createTemporalTableCmd.MarkFlagRequired("temp-table-name")

	createTemporalTableCmd.Flags().StringP("temp-table-ttl", "", "", "TTL of the destination table (hours) (optional, default: 12h)")

	createTemporalTableCmd.Flags().StringP("query", "", "", "The query to execute")
	createTemporalTableCmd.MarkFlagRequired("query")



	viper.BindPFlag("create-temporal-table-query", createTemporalTableCmd.Flags().Lookup("query"))
	viper.BindPFlag("create-temporal-table-project-id", createTemporalTableCmd.Flags().Lookup("project-id"))
	viper.BindPFlag("create-temporal-table-dataset-id", createTemporalTableCmd.Flags().Lookup("temp-dataset-id"))
	viper.BindPFlag("create-temporal-table-name", createTemporalTableCmd.Flags().Lookup("temp-table-name"))
	viper.BindPFlag("create-temporal-table-ttl", createTemporalTableCmd.Flags().Lookup("temp-table-ttl"))

	rootCmd.AddCommand(createTemporalTableCmd)
}

var createTemporalTableCmd = &cobra.Command{
	Use:   "create-temporal-table",
	Short: "Create temporal table",
	Long:  `Create temporal table
Format:
	bigquery create-temporal-table-sql --project-id= --sql= 
Example:
	bigquery create-temporal-table-sql \
	  --project-id=world-fishing \
	  --sql="SELECT * FROM vessels;"
`,
	Run: func(cmd *cobra.Command, args []string) {
		log.Println("→ Executing create temporal table command")

		params := types.ExecuteCreateTemporalTableParams{
			Query:        	viper.GetString("create-temporal-table-query"),
			ProjectId:  	viper.GetString("create-temporal-table-project-id"),
			TempTableName:  viper.GetString("create-temporal-table-name"),
			TempDatasetId:  viper.GetString("create-temporal-table-dataset-id"),
			TTL:            viper.GetInt("create-temporal-table-ttl"),
		}
		log.Println(params)

		action.ExecuteCreateTemporalTable(params)
		log.Println("→ Executing create temporal table finished")
	},
}

