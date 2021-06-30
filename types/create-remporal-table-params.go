package types

type ExecuteRaqSqlParams struct {
	Query         string
	ProjectId 	  string
	TempDatasetId string
	TempTableName string
	Schema        string
	TTL           int
}