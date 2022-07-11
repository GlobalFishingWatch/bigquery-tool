package action

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/GlobalFishingWatch/bigquery-tool/types"
	"google.golang.org/api/iterator"
)

func ExecuteRawQuery(params types.ExecuteRawQueryParams) []map[string]interface{} {
	ctx := context.Background()

	bigQueryClient = createBigQueryClient(ctx, params.ProjectId)
	defer bigQueryClient.Close()
	if params.DestinationDataset != "" {
		executeDestinationQuery(ctx, bigQueryClient, params)
		return nil
	} else {
		results := executeQuery(ctx, bigQueryClient, params)
		return results
	}
}

func executeDestinationQuery(ctx context.Context, client *bigquery.Client, params types.ExecuteRawQueryParams) {

	log.Printf("→ BQ →→ Executing query with destination table %s.%s", params.DestinationDataset, params.DestinationTable)
	dstTable := client.Dataset(params.DestinationDataset).Table(params.DestinationTable)
	query := client.Query(params.Query)
	query.QueryConfig.Dst = dstTable
	query.QueryConfig.WriteDisposition = bigquery.TableWriteDisposition(params.WriteDisposition)

	job, err := query.Run(context.Background())
	if err != nil {
		log.Fatalf("→ BQ →→ Error running query %e", err)
	}
	for {
		log.Println("→ BQ →→ Checking status of job")
		status, err := job.Status(context.Background())
		if err != nil {
			log.Fatalf("→ BQ →→ Error obtaining status %e", err)
		}
		log.Println("→ BQ →→ Done:", status.Done())
		if status.Done() {
			if len(status.Errors) > 0 {
				log.Fatalf("→ BQ →→ Error importing data %v", status.Errors)
			}
			break
		}
		time.Sleep(5 * time.Second)
	}
	log.Println("→ BQ →→ Query run correctly")
}

func executeQuery(ctx context.Context, bigQueryClient *bigquery.Client, params types.ExecuteRawQueryParams) []map[string]interface{} {
	query := bigQueryClient.Query(params.Query)
	query.AllowLargeResults = true
	query.Dst = nil

	log.Println("→ BQ →→ Executing query")
	it, err := query.Read(ctx)
	if err != nil {
		log.Fatal(err)
	}
	var rows []map[string]bigquery.Value
	for {
		row := make(map[string]bigquery.Value)
		err := it.Next(&row)
		if err == iterator.Done {
			break
		}
		if err != nil {
			log.Fatal(err)
		}
		rows = append(rows, row)
	}

	log.Println("→ BQ →→ Parsing bigquery.values to bytes")
	result, err := json.Marshal(rows)
	if err != nil {
		log.Fatal(err)
	}

	log.Println("→ BQ →→ Parsing bytes to json")
	var resultParsed []map[string]interface{}
	err = json.Unmarshal(result, &resultParsed)
	if err != nil {
		log.Fatal(err)
	}

	return resultParsed
}
