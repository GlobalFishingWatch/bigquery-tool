package action

import (
	"cloud.google.com/go/bigquery"
	"context"
	"encoding/json"
	"github.com/GlobalFishingWatch/bigquery-tool/types"
	"google.golang.org/api/iterator"
	"log"
)


func ExecuteRawQuery(params types.ExecuteRawQueryParams) []map[string]interface{} {
	ctx := context.Background()

	bigQueryClient = createBigQueryClient(ctx, params.ProjectId)
	defer bigQueryClient.Close()

	results := executeQuery(ctx, bigQueryClient, params)
	return results
}


func executeQuery(ctx context.Context, bigQueryClient *bigquery.Client, params types.ExecuteRawQueryParams) []map[string]interface{} {
	query := bigQueryClient.Query(params.Query)
	query.AllowLargeResults = true

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

