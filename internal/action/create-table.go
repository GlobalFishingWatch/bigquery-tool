package action

import (
	"cloud.google.com/go/bigquery"
	"context"
	"github.com/GlobalFishingWatch/bigquery-tool/types"
	"log"
)

func ExecuteCreateTable(params types.ExecuteCreateTableParams) {
	ctx := context.Background()

	bigQueryClient = createBigQueryClient(ctx, params.ProjectId)
	defer bigQueryClient.Close()

	createTable(ctx, params)
}


func createTable(ctx context.Context, params types.ExecuteCreateTableParams) {
	query := bigQueryClient.Query(params.Query)
	query.AllowLargeResults = true

	log.Printf("→ BQ →→  table name: %s:%s.%s", params.ProjectId, params.DatasetId, params.TableName)
	dstTable := bigQueryClient.Dataset(params.DatasetId).Table(params.TableName)

	err := dstTable.Create(ctx, &bigquery.TableMetadata{})
	if err != nil {
		log.Fatal("→ BQ →→ Error creating temporary table", err)
	}

	job, err := query.Run(context.Background())
	checkBigQueryJob(job, err)
	config, err := job.Config()
	if err != nil {
		log.Fatal("→ BQ →→ Error obtaining config", err)
	}
	tempTable := config.(*bigquery.QueryConfig).Dst
	log.Println("→ BQ →→ Temp table id", tempTable.TableID)
}

