package types

type ExecuteCreateTemporalTableParams struct {
	Query         string
	ProjectId 	  string
	TempDatasetId string
	TempTableName string
	TTL           int
}