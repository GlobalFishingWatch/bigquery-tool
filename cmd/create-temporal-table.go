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
	createTemporalTableCmd.Flags().StringP("temp-table-schema", "", "", "The schema of the destination table")

	createTemporalTableCmd.Flags().StringP("query", "", "", "The query to execute")
	createTemporalTableCmd.MarkFlagRequired("query")



	viper.BindPFlag("create-temporal-table-query", createTemporalTableCmd.Flags().Lookup("query"))
	viper.BindPFlag("create-temporal-table-project-id", createTemporalTableCmd.Flags().Lookup("project-id"))
	viper.BindPFlag("create-temporal-table-dataset-id", createTemporalTableCmd.Flags().Lookup("temp-dataset-id"))
	viper.BindPFlag("create-temporal-table-name", createTemporalTableCmd.Flags().Lookup("temp-table-name"))
	viper.BindPFlag("create-temporal-table-ttl", createTemporalTableCmd.Flags().Lookup("temp-table-ttl"))
	viper.BindPFlag("create-temporal-table-schema", createTemporalTableCmd.Flags().Lookup("temp-table-schema"))

	rootCmd.AddCommand(createTemporalTableCmd)
}

var createTemporalTableCmd = &cobra.Command{
	Use:   "create-temporal-table",
	Short: "Execute raw sql",
	Long:  `Execute raw sql
Format:
	postgres create-temporal-table-sql --project-id= --sql= 
Example:
	postgres create-temporal-table-sql \
	  --project-id=world-fishing \
	  --sql="SELECT * FROM vessels;"
`,
	Run: func(cmd *cobra.Command, args []string) {
		log.Println("→ Executing create temporal table command")

		params := types.ExecuteRaqSqlParams{
			Query:        	viper.GetString("create-temporal-table-query"),
			ProjectId:  	viper.GetString("create-temporal-table-project-id"),
			TempTableName:  viper.GetString("create-temporal-table-name"),
			TempDatasetId:  viper.GetString("create-temporal-table-dataset-id"),
			TTL:            viper.GetInt("create-temporal-table-ttl"),
			Schema:         viper.GetString("create-temporal-table-schema"),
		}
		action.ExecuteRawSql(params)
		log.Println("→ Executing create temporal table finished")
	},
}

