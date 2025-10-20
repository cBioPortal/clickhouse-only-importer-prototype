package cna

import (
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// public interface
func ProcessTSVToParquet(
	tsvPath, outputPath string,
	schema *arrow.Schema,
	mem memory.Allocator,
	studyID, profileID string,
) error {
	recordChan, errChan := transformTSVStream(tsvPath, schema, mem, studyID, profileID)

	// write in parallel with transform
	writeErr := WriteParquetStream(outputPath, recordChan, mem)

	// check for transform errors
	if transformErr := <-errChan; transformErr != nil {
		return transformErr
	}

	// check for write errors
	if writeErr != nil {
		return writeErr
	}

	return nil
}

// transformTSVStream reads records from TSV and transforms them into arrow record batch
func transformTSVStream(
	tsvPath string,
	schema *arrow.Schema,
	mem memory.Allocator,
	studyID, profileID string,
) (<-chan arrow.RecordBatch, <-chan error) {
	recordChan := make(chan arrow.RecordBatch, 10) // buffer a few records
	errChan := make(chan error, 1)

	go func() {
		defer close(recordChan)
		defer close(errChan)

		rdr, file, err := ReadTSVAsRecords(tsvPath, schema, 100, '\t')
		if err != nil {
			errChan <- fmt.Errorf("failed to open TSV: %w", err)
			return
		}
		defer file.Close()
		defer rdr.Release()

		for rdr.Next() {
			rec := rdr.RecordBatch()
			if rec == nil {
				continue
			}

			tr, err := transformCNARecordBatch(studyID, profileID, rec, mem)
			rec.Release()

			if err != nil {
				errChan <- fmt.Errorf("transform failed: %w", err)
				return
			}

			// send to channel (blocks if buffer full - provides backpressure)
			recordChan <- tr
		}

		if err := rdr.Err(); err != nil {
			errChan <- fmt.Errorf("TSV reader error: %w", err)
		}
	}()

	return recordChan, errChan
}

// cnaTableSchema is the private schema used for all cna tables.
var cnaTableSchema = arrow.NewSchema([]arrow.Field{
	{Name: "SAMPLE_ID", Type: arrow.BinaryTypes.String},
	{Name: "CANCER_STUDY", Type: arrow.BinaryTypes.String},
	{Name: "GENE_SYMBOL", Type: arrow.BinaryTypes.String},
	{Name: "GENETIC_PROFILE", Type: arrow.BinaryTypes.String},
	{Name: "ALTERATION", Type: arrow.BinaryTypes.String},
}, nil)

// transformCNARecordBatch converts one tsv record batch into cna-format arrow record
func transformCNARecordBatch(
	cancerStudyName, geneticProfileName string,
	rec arrow.RecordBatch,
	mem memory.Allocator,
) (arrow.RecordBatch, error) {
	bldr := array.NewRecordBuilder(mem, cnaTableSchema)
	defer bldr.Release()

	sampleIDBldr := bldr.Field(0).(*array.StringBuilder)
	studyBldr := bldr.Field(1).(*array.StringBuilder)
	geneBldr := bldr.Field(2).(*array.StringBuilder)
	profileBldr := bldr.Field(3).(*array.StringBuilder)
	alterationBldr := bldr.Field(4).(*array.StringBuilder)

	geneSymbols, ok := rec.Column(0).(*array.String)
	if !ok {
		return nil, fmt.Errorf("first column (gene symbols) is not a string array")
	}

	prefix := cancerStudyName + "_"
	numCols := int(rec.NumCols())
	numRows := int(rec.NumRows())

	for colIdx := 1; colIdx < numCols; colIdx++ {
		colName := rec.ColumnName(colIdx)
		alterations, ok := rec.Column(colIdx).(*array.String)
		if !ok {
			return nil, fmt.Errorf("column %q is not a string array", colName)
		}

		sampleID := prefix + colName
		for rowIdx := 0; rowIdx < numRows; rowIdx++ {
			sampleIDBldr.Append(sampleID)
			studyBldr.Append(cancerStudyName)
			geneBldr.Append(geneSymbols.Value(rowIdx))
			profileBldr.Append(geneticProfileName)
			alterationBldr.Append(alterations.Value(rowIdx))
		}
	}

	// create output record
	recOut := bldr.NewRecordBatch()

	// validation step
	expected := recOut.Column(0).Len()
	for i := 1; i < int(recOut.NumCols()); i++ {
		col := recOut.Column(i)
		if col.Len() != expected {
			recOut.Release()
			return nil, fmt.Errorf(
				"column %q length mismatch: %d vs %d",
				recOut.ColumnName(i), col.Len(), expected,
			)
		}
	}

	return recOut, nil
}
