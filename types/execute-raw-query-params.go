package types

type ExecuteRawQueryParams struct {
	Query              string
	ProjectId          string
	DestinationTable   string
	DestinationDataset string
	WriteDisposition   string
}
