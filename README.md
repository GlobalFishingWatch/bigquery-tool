# bigquery-tool

## Description

bigquery-tool is an agnostic CLI to expose commands which allows you manage actions using BQ

Format:
```
bigquery-tool [command] [--flags]
```

### Tech Stack:
* [Golang](https://golang.org/doc/)
* [Cobra Framework](https://github.com/spf13/cobra#working-with-flags)
* [Viper](https://github.com/spf13/viper)
* [Docker](https://docs.docker.com/)

### Git
* Repository:
  https://github.com/GlobalFishingWatch/bigquery-tool

## Usage

There are available the following commands:
* create-temporal-table

---

### Command: [create-temporal-table]

The create-temporal-table command allows you to generate a temporal table from a Query.

#### Flags
##### Required flags
- `--project-id=` the project id of the destination table.
- `--temp-dataset-id=` The destination dataset.
- `--temp-table-name=` The destination name table.
- `--query=`SQL query to get rows from BigQuery.

##### Optional flags
- `--temp-table-ttl=` The expiration time of the table (hours, default: 12h)


#### Example
Here an example of this command:
```
bigquery create-temporal table \
   --project-id="world-fishing-827" \
   --temp-dataset-id="scratch_alvaro" \ 
   --temp-table-name="test-tool" \
   --query="SELECT event_id FROM \`world-fishing-827.scratch_alvaro.published_events_fishing\` LIMIT 2" 
   --temp-table-ttl=2
 
```

When you execute this command, under the hood happens the followings steps:
* The CLI creates a temporal table
* The CLI adds ttl to the created table
* The CLI executes the SQL query and insert the rows
---

### Command: [create-table]

The create-table command allows you to generate a table from a Query.

#### Flags
##### Required flags
- `--project-id=` the project id of the destination table.
- `--dataset-id=` The destination dataset.
- `--table-name=` The destination name table.
- `--query=`SQL query to get rows from BigQuery.

##### Optional flags


#### Example
Here an example of this command:
```
bigquery create-temporal table \
   --project-id="world-fishing-827" \
   --dataset-id="scratch_alvaro" \ 
   --table-name="test-tool" \
   --query="SELECT event_id FROM \`world-fishing-827.scratch_alvaro.published_events_fishing\` LIMIT 2"  
```

When you execute this command, under the hood happens the followings steps:
* The CLI creates a temporal table
* The CLI adds ttl to the created table
* The CLI executes the SQL query and insert the rows

