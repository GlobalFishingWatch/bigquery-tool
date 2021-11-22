package pkg

import (
	"github.com/GlobalFishingWatch/bigquery-tool/internal/action"
	"github.com/GlobalFishingWatch/bigquery-tool/types"
)

func CreateTable(params types.ExecuteCreateTableParams) {
	action.ExecuteCreateTable(params)
}

func CreateTemporalTable(params types.ExecuteCreateTemporalTableParams) {
	action.ExecuteCreateTemporalTable(params)
}

func ExecuteRawQuery(params types.ExecuteRawQueryParams) []map[string]interface{} {
	return action.ExecuteRawQuery(params)
}
