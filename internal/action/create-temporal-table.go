package action

import (
	"cloud.google.com/go/bigquery"
	"context"
	"github.com/GlobalFishingWatch/bigquery-tool/types"
	"log"
	"time"
)


func ExecuteCreateTemporalTable(params types.ExecuteCreateTemporalTableParams) {
	ctx := context.Background()

	bigQueryClient = createBigQueryClient(ctx, params.ProjectId)
	defer bigQueryClient.Close()

	createTemporalTable(ctx, bigQueryClient, params)
}


func createTemporalTable(ctx context.Context, bigQueryClient *bigquery.Client, params types.ExecuteCreateTemporalTableParams) {
	log.Printf("→ BQ →→ Query: %s", params.Query)
	query := bigQueryClient.Query(params.Query)
	query.AllowLargeResults = true

	log.Printf("→ BQ →→ Temporal table name: %s:%s.%s", params.ProjectId, params.TempDatasetId, params.TempTableName)
	dstTable := bigQueryClient.Dataset(params.TempDatasetId).Table(params.TempTableName)

	var ttlParsed time.Duration
	if params.TTL == 0 {
		ttlParsed = 12 * time.Hour
	} else {
		ttlParsed = time.Duration(params.TTL) * time.Hour
	}
	log.Printf("→ BQ →→ Temporal table TTL: %v", ttlParsed)

	var tableMetadata *bigquery.TableMetadata
	tableMetadata = &bigquery.TableMetadata{ExpirationTime: time.Now().Add(ttlParsed)}

	err := dstTable.Create(ctx, tableMetadata)
	if err != nil {
		log.Fatal("→ BQ →→ Error creating temporary table", err)
	}

	query.QueryConfig.Dst = dstTable
	log.Println("→ BQ →→ Exporting query to intermediate table")
	job, err := query.Run(context.Background())
	checkBigQueryJob(job, err)
	config, err := job.Config()
	if err != nil {
		log.Fatal("→ BQ →→ Error obtaining config", err)
	}
	tempTable := config.(*bigquery.QueryConfig).Dst
	log.Println("→ BQ →→ Temp table id", tempTable.TableID)
}