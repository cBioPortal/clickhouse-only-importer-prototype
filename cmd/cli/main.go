package main

import (
	"log"
	"os"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/cbioportal/clickhouse-only-importer-prototype/cna"
)

func main() {
	// Configuration constants
	const (
		cnaTsvPath  = ""                     // path to data_CNA.txt
		parquetPath = ""                     // path to output-cna.parquet
		studyID     = "genie_private"        // study id used within record - genie_private
		profileID   = "genie_private_gistic" // profile id used within record - genie_private_gistic
	)

	mem := memory.DefaultAllocator

	log.Print("getting sample ids from cna file")
	f, err := os.Open(cnaTsvPath)
	if err != nil {
		panic(err)
	}
	schema, err := cna.GetFieldTypesFromFile(f, '\t')
	f.Close()
	if err != nil {
		panic(err)
	}

	log.Print("transforming tsv and writing to parquet (streaming)")
	if err := cna.ProcessTSVToParquet(cnaTsvPath, parquetPath, schema, mem, studyID, profileID); err != nil {
		panic(err)
	}

	log.Print("Conversion complete:", parquetPath)
}
