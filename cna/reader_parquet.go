package cna

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

// readParquetAsRecords streams arrow record batches from a parquet file
func readParquetAsRecords(
	parquetPath string,
	mem memory.Allocator,
) (<-chan arrow.RecordBatch, <-chan error) {
	recordChan := make(chan arrow.RecordBatch, 10)
	errChan := make(chan error, 1)

	go func() {
		defer close(recordChan)
		defer close(errChan)

		// open the parquet file
		f, err := os.Open(parquetPath)
		if err != nil {
			errChan <- fmt.Errorf("failed to open parquet file %s: %w", parquetPath, err)
			return
		}
		defer f.Close()

		// get file info for size
		stat, err := f.Stat()
		if err != nil {
			errChan <- fmt.Errorf("failed to stat file %s: %w", parquetPath, err)
			return
		}

		// create parquet file reader
		pf, err := file.NewParquetReader(f)
		if err != nil {
			errChan <- fmt.Errorf("failed to create parquet reader for %s: %w", parquetPath, err)
			return
		}
		defer pf.Close()

		// create arrow file reader
		arrowReader, err := pqarrow.NewFileReader(pf, pqarrow.ArrowReadProperties{}, mem)
		if err != nil {
			errChan <- fmt.Errorf("failed to create arrow reader for %s: %w", parquetPath, err)
			return
		}

		// read the entire file as a table
		tbl, err := arrowReader.ReadTable(context.Background())
		if err != nil {
			errChan <- fmt.Errorf("failed to read table from %s: %w", parquetPath, err)
			return
		}
		defer tbl.Release()

		// convert table to record batches
		tr := array.NewTableReader(tbl, int64(stat.Size()))
		defer tr.Release()

		for tr.Next() {
			batch := tr.Record()
			batch.Retain() // Retain before sending to channel
			recordChan <- batch
		}

		if err := tr.Err(); err != nil {
			errChan <- fmt.Errorf("error reading table from %s: %w", parquetPath, err)
			return
		}
	}()

	return recordChan, errChan
}

// CombineParquetFilesByPattern combines multiple parquet files matching a glob pattern into a single parquet file
func CombineParquetFilesByPattern(
	pattern string,
	outputPath string,
	mem memory.Allocator,
) error {
	parquetFiles, err := filepath.Glob(pattern)
	if err != nil {
		return fmt.Errorf("failed to find parquet files: %w", err)
	}

	if len(parquetFiles) == 0 {
		return fmt.Errorf("no parquet files found matching pattern %s", pattern)
	}

	// filter out the output file if it already exists in the same directory
	var filteredFiles []string
	for _, f := range parquetFiles {
		if f != outputPath {
			filteredFiles = append(filteredFiles, f)
		}
	}
	parquetFiles = filteredFiles

	if len(parquetFiles) == 0 {
		return fmt.Errorf("no input parquet files to combine")
	}

	log.Printf("Found %d files to combine", len(parquetFiles))

	// create a channel to aggregate all records
	combinedChan := make(chan arrow.RecordBatch, 10)

	go func() {
		defer close(combinedChan)

		for _, parquetFile := range parquetFiles {
			log.Printf("processing: %s", parquetFile)
			recordChan, errChan := readParquetAsRecords(parquetFile, mem)

			// forward all records from this file
			for rec := range recordChan {
				combinedChan <- rec
			}

			// check for errors
			if err := <-errChan; err != nil {
				// note: we can't return error from goroutine, so we log it
				// in production, you might want a better error handling mechanism
				fmt.Printf("error reading %s: %v\n", parquetFile, err)
				return
			}
		}
	}()

	// Write all records to the combined parquet file
	return WriteParquetStream(outputPath, combinedChan, mem)
}
