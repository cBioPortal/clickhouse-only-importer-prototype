package cna

import (
	"fmt"
	"log"
	"path/filepath"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// CNAFileInput represents a single CNA file with its schema
type CNAFileInput struct {
	Path             string
	Schema           *arrow.Schema
	CancerStudyId    string
	GeneticProfileId string
}

// ProcessMultipleTSVToParquet processes multiple CNA TSV files and writes each to its own parquet
// file
func ProcessMultipleTSVToParquet(
	cnaFiles []CNAFileInput,
	outputDir string,
	mem memory.Allocator,
) error {
	for _, cnaFile := range cnaFiles {
		log.Printf("processing: %s", cnaFile.Path)

		// generate output filename based on input file path
		outputPath := generateOutputPath(cnaFile.Path, outputDir)

		if err := ProcessSingleTSVToParquet(cnaFile, outputPath, mem); err != nil {
			return fmt.Errorf("error processing %s: %w", cnaFile.Path, err)
		}

		log.Printf("written: %s", outputPath)
	}

	return nil
}

// ProcessSingleTSVToParquet processes a single CNA TSV file and writes to a single parquet file
func ProcessSingleTSVToParquet(
	cnaFile CNAFileInput,
	outputPath string,
	mem memory.Allocator,
) error {
	recordChan := make(chan arrow.RecordBatch, 10)
	errChan := make(chan error, 1)

	// start goroutine to process the file
	go func() {
		defer close(recordChan)
		defer close(errChan)

		// get records from this file
		fileChan, fileErrChan := transformTSVStream(
			cnaFile.Path,
			cnaFile.Schema,
			mem,
			cnaFile.CancerStudyId,
			cnaFile.GeneticProfileId,
		)

		// forward all records from this file to the channel
		for rec := range fileChan {
			recordChan <- rec
		}

		// check for errors from this file
		if err := <-fileErrChan; err != nil {
			errChan <- fmt.Errorf("error processing %s: %w", cnaFile.Path, err)
			return
		}
	}()

	// write all records to parquet file
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

// generateOutputPath creates an output path for a parquet file based on the input TSV path
func generateOutputPath(inputPath, outputDir string) string {
	// get the study dir name
	dir := filepath.Base(filepath.Dir(inputPath))
	// get the base filename without extension
	base := filepath.Base(inputPath)
	// remove .txt extension and replace with .parquet
	nameWithoutExt := strings.TrimSuffix(base, filepath.Ext(base))
	// combine directory name (study) and filename
	outputFilename := dir + "_" + nameWithoutExt + ".parquet"

	return filepath.Join(outputDir, outputFilename)
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

	// we start at index 2 because 0:hugo, 1:entrez
	for colIdx := 2; colIdx < numCols; colIdx++ {
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
