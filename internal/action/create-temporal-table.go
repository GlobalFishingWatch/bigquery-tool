package action

import (
	"cloud.google.com/go/bigquery"
	"context"
	"github.com/GlobalFishingWatch/bigquery-tool/internal/common"
	"github.com/GlobalFishingWatch/bigquery-tool/types"
	"log"
	"time"
)

var bigQueryClient *bigquery.Client

func ExecuteRawSql(params types.ExecuteRaqSqlParams) {
	ctx := context.Background()

	bigQueryClient = common.CreateBigQueryClient(ctx, params.ProjectId)
	defer bigQueryClient.Close()

	createTemporalTable(ctx, params)
}


func createTemporalTable(ctx context.Context, params types.ExecuteRaqSqlParams) {
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

	err := dstTable.Create(ctx, &bigquery.TableMetadata{ExpirationTime: time.Now().Add(ttlParsed)})
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

func checkBigQueryJob(job *bigquery.Job, err error) {
	if err != nil {
		log.Fatal("→ BQ →→ Error creating job", err)
	}
	for {
		log.Println("→ BQ →→ Checking status of job")
		status, err := job.Status(context.Background())
		if err != nil {
			log.Fatal("→ BQ →→ Error obtaining status", err)
		}
		log.Println("Done:", status.Done())
		if status.Done() {
			if len(status.Errors) > 0 {
				log.Fatal("Error", status.Errors)
			}
			break
		}
		time.Sleep(5 * time.Second)
	}
}