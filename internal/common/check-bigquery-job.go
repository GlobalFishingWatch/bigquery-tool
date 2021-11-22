package common

import (
	"context"
	"log"
	"time"
	"cloud.google.com/go/bigquery"
)

func CheckBigQueryJob(job *bigquery.Job, err error) {
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