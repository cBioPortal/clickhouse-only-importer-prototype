# clickhouse-only-importer-prototype

convert mode (converts tsv to parquet):<br>
go run ./cmd/cli/main.go -mode convert -tsv-dir ./data -parquet-dir ./output<br>

where:<br>
-tsv-dir: is the path to the root directory containing study data (/datahub/public)<br>
-parquet-dir: is the path to place individual cna parquet files<br>

combine mode (combines individual parquets to a single parquet):<br>
go run ./cmd/cli/main.go -mode combine -parquet-dir ./output -output final.parquet<br>

where:<br>
-parquet-dir: specifies path where individual cna parquet files reside<br>
-output: full path & filename specifies where to place combined parquet<br>
