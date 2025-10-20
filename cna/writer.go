package cna

import (
	"fmt"
	"os"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

func WriteParquetStream(
	filename string,
	recordChan <-chan arrow.RecordBatch,
	mem memory.Allocator,
) error {
	outFile, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("cannot create parquet file: %w", err)
	}
	defer outFile.Close()

	pqProps := parquet.NewWriterProperties(
		parquet.WithAllocator(mem),
		parquet.WithCompression(compress.Codecs.Snappy),
	)
	arrowProps := pqarrow.DefaultWriterProps()

	var writer *pqarrow.FileWriter
	defer func() {
		if writer != nil {
			writer.Close()
		}
	}()

	// consume records from channel
	for rec := range recordChan {
		// initialize writer with schema from first record
		if writer == nil {
			writer, err = pqarrow.NewFileWriter(rec.Schema(), outFile, pqProps, arrowProps)
			if err != nil {
				rec.Release()
				return fmt.Errorf("cannot create writer: %w", err)
			}
		}

		// write and release immediately
		if err := writer.Write(rec); err != nil {
			rec.Release()
			return fmt.Errorf("cannot write record: %w", err)
		}
		rec.Release()
	}

	return nil
}
