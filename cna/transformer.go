package cna

import (
	"fmt"
	"log"
	"path/filepath"
	"strings"
	"sync"

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

// ProcessMultipleTSVToParquet processes multiple CNA TSV files and writes each to two parquet files:
// one for genetic alterations and one for genetic profile samples
func ProcessMultipleTSVToParquet(
	cnaFiles []CNAFileInput,
	outputDir string,
	mem memory.Allocator,
) error {
	for _, cnaFile := range cnaFiles {
		log.Printf("processing: %s", cnaFile.Path)

		// generate output filenames based on input file path
		alterationsPath, samplesPath := generateOutputPaths(cnaFile.Path, outputDir)

		if err := ProcessSingleTSVToTwoParquets(cnaFile, alterationsPath, samplesPath, mem); err != nil {
			return fmt.Errorf("error processing %s: %w", cnaFile.Path, err)
		}

		log.Printf("written: %s", alterationsPath)
		log.Printf("written: %s", samplesPath)
	}

	return nil
}

// ProcessMultipleTSVToParquetWithDerived processes multiple CNA TSV files and writes each to three parquet files:
// one for genetic alterations, one for genetic profile samples, and one for derived denormalized format
func ProcessMultipleTSVToParquetWithDerived(
	cnaFiles []CNAFileInput,
	outputDir string,
	mem memory.Allocator,
) error {
	for _, cnaFile := range cnaFiles {
		log.Printf("processing: %s", cnaFile.Path)

		// generate output filenames based on input file path
		alterationsPath, samplesPath, derivedPath := generateOutputPathsWithDerived(cnaFile.Path, outputDir)

		if err := ProcessSingleTSVToThreeParquets(cnaFile, alterationsPath, samplesPath, derivedPath, mem); err != nil {
			return fmt.Errorf("error processing %s: %w", cnaFile.Path, err)
		}

		log.Printf("written: %s", alterationsPath)
		log.Printf("written: %s", samplesPath)
		log.Printf("written: %s", derivedPath)
	}

	return nil
}

func ProcessSingleTSVToTwoParquets(
	cnaFile CNAFileInput,
	alterationsPath string,
	samplesPath string,
	mem memory.Allocator,
) error {
	alterationsChan := make(chan arrow.RecordBatch, 10)
	samplesChan := make(chan arrow.RecordBatch, 10)

	// channel for all errors
	errChan := make(chan error, 4) // transform + 2 writers + forwarding

	// start transform and forwarding
	go func() {
		defer close(alterationsChan)
		defer close(samplesChan)

		alterationsFileChan, samplesFileChan, fileErrChan := transformTSVStreamToTwo(
			cnaFile.Path,
			cnaFile.Schema,
			mem,
			cnaFile.CancerStudyId,
			cnaFile.GeneticProfileId,
		)

		var wg sync.WaitGroup
		wg.Add(2)

		// forward alterations
		go func() {
			defer wg.Done()
			for rec := range alterationsFileChan {
				alterationsChan <- rec
			}
		}()

		// forward samples
		go func() {
			defer wg.Done()
			for rec := range samplesFileChan {
				samplesChan <- rec
			}
		}()

		wg.Wait()

		// check for transform errors
		if err := <-fileErrChan; err != nil {
			errChan <- fmt.Errorf("error processing %s: %w", cnaFile.Path, err)
		}
	}()

	// write both parquet files concurrently
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		if err := WriteParquetStream(alterationsPath, alterationsChan, mem); err != nil {
			errChan <- err
		}
	}()

	go func() {
		defer wg.Done()
		if err := WriteParquetStream(samplesPath, samplesChan, mem); err != nil {
			errChan <- err
		}
	}()

	// wait for all operations to complete
	wg.Wait()
	close(errChan)

	// Collect all errors
	var errs []error
	for err := range errChan {
		errs = append(errs, err)
	}

	if len(errs) > 0 {
		return fmt.Errorf("errors: %v", errs)
	}

	return nil
}

// ProcessSingleTSVToThreeParquets processes a single CNA TSV file and writes to three parquet files:
// one for genetic alterations and one for genetic profile samples and one derived
func ProcessSingleTSVToThreeParquets(
	cnaFile CNAFileInput,
	alterationsPath string,
	samplesPath string,
	derivedPath string,
	mem memory.Allocator,
) error {
	alterationsChan := make(chan arrow.RecordBatch, 10)
	samplesChan := make(chan arrow.RecordBatch, 10)
	derivedChan := make(chan arrow.RecordBatch, 10)

	// channel for all errors
	errChan := make(chan error, 6) // transform + 3 writers + forwarding

	// start transform and forwarding
	go func() {
		defer close(alterationsChan)
		defer close(samplesChan)
		defer close(derivedChan)

		alterationsFileChan, samplesFileChan, derivedFileChan, fileErrChan := transformTSVStreamToThree(
			cnaFile.Path,
			cnaFile.Schema,
			mem,
			cnaFile.CancerStudyId,
			cnaFile.GeneticProfileId,
		)

		var wg sync.WaitGroup
		wg.Add(3)

		// forward alterations
		go func() {
			defer wg.Done()
			for rec := range alterationsFileChan {
				alterationsChan <- rec
			}
		}()

		// forward samples
		go func() {
			defer wg.Done()
			for rec := range samplesFileChan {
				samplesChan <- rec
			}
		}()

		// forward derived
		go func() {
			defer wg.Done()
			for rec := range derivedFileChan {
				derivedChan <- rec
			}
		}()

		wg.Wait()

		// check for transform errors
		if err := <-fileErrChan; err != nil {
			errChan <- fmt.Errorf("error processing %s: %w", cnaFile.Path, err)
		}
	}()

	// write all three parquet files concurrently
	var wg sync.WaitGroup
	wg.Add(3)

	go func() {
		defer wg.Done()
		if err := WriteParquetStream(alterationsPath, alterationsChan, mem); err != nil {
			errChan <- err
		}
	}()

	go func() {
		defer wg.Done()
		if err := WriteParquetStream(samplesPath, samplesChan, mem); err != nil {
			errChan <- err
		}
	}()

	go func() {
		defer wg.Done()
		if err := WriteParquetStream(derivedPath, derivedChan, mem); err != nil {
			errChan <- err
		}
	}()

	// wait for all operations to complete
	wg.Wait()
	close(errChan)

	// Collect all errors
	var errs []error
	for err := range errChan {
		errs = append(errs, err)
	}

	if len(errs) > 0 {
		return fmt.Errorf("errors: %v", errs)
	}

	return nil
}

// generateOutputPaths creates two output paths: one for genetic alterations and one for genetic profile samples
func generateOutputPaths(inputPath, outputDir string) (alterationsPath, samplesPath string) {
	// get the study dir name
	dir := filepath.Base(filepath.Dir(inputPath))
	// get the base filename without extension
	base := filepath.Base(inputPath)
	// remove .txt extension
	nameWithoutExt := strings.TrimSuffix(base, filepath.Ext(base))

	// create two different output filenames
	alterationsFilename := dir + "_" + nameWithoutExt + "_genetic_alterations.parquet"
	samplesFilename := dir + "_" + nameWithoutExt + "_genetic_profile_samples.parquet"

	return filepath.Join(outputDir, alterationsFilename), filepath.Join(outputDir, samplesFilename)
}

// generateOutputPathsWithDerived creates three output paths: genetic alterations, genetic profile samples, and derived
func generateOutputPathsWithDerived(inputPath, outputDir string) (alterationsPath, samplesPath, derivedPath string) {
	// get the study dir name
	dir := filepath.Base(filepath.Dir(inputPath))
	// get the base filename without extension
	base := filepath.Base(inputPath)
	// remove .txt extension
	nameWithoutExt := strings.TrimSuffix(base, filepath.Ext(base))

	// create three different output filenames
	alterationsFilename := dir + "_" + nameWithoutExt + "_genetic_alterations.parquet"
	samplesFilename := dir + "_" + nameWithoutExt + "_genetic_profile_samples.parquet"
	derivedFilename := dir + "_" + nameWithoutExt + "_derived.parquet"

	return filepath.Join(outputDir, alterationsFilename), filepath.Join(outputDir, samplesFilename), filepath.Join(outputDir, derivedFilename)
}

// transformTSVStreamToTwo reads records from TSV and transforms them into two arrow record batch streams:
// one for genetic alterations and one for genetic profile samples
func transformTSVStreamToTwo(
	tsvPath string,
	schema *arrow.Schema,
	mem memory.Allocator,
	studyID, profileID string,
) (<-chan arrow.RecordBatch, <-chan arrow.RecordBatch, <-chan error) {
	alterationsRecordChan := make(chan arrow.RecordBatch, 10) // buffer a few records
	samplesRecordChan := make(chan arrow.RecordBatch, 10)     // buffer a few records
	errChan := make(chan error, 1)

	go func() {
		defer close(alterationsRecordChan)
		defer close(samplesRecordChan)
		defer close(errChan)

		rdr, file, err := ReadTSVAsRecords(tsvPath, schema, 100, '\t')
		if err != nil {
			errChan <- fmt.Errorf("failed to open TSV: %w", err)
			return
		}
		defer file.Close()
		defer rdr.Release()

		// Track if we've sent the samples record (should only send once)
		samplesSent := false

		for rdr.Next() {
			rec := rdr.RecordBatch()
			if rec == nil {
				continue
			}

			gar, gps, err := transformCNARecordBatch(studyID, profileID, rec, mem)
			rec.Release()

			if err != nil {
				errChan <- fmt.Errorf("transform failed: %w", err)
				return
			}

			// Always send genetic alterations record
			alterationsRecordChan <- gar

			// Only send genetic profile samples record on first batch
			if !samplesSent {
				samplesRecordChan <- gps
				samplesSent = true
			} else {
				// Release subsequent samples records since we don't need them
				gps.Release()
			}
		}

		if err := rdr.Err(); err != nil {
			errChan <- fmt.Errorf("TSV reader error: %w", err)
		}
	}()

	return alterationsRecordChan, samplesRecordChan, errChan
}

// transformTSVStreamToThree reads records from TSV and transforms them into three arrow record batch streams:
// one for genetic alterations, one for genetic profile samples, and one for derived denormalized format
func transformTSVStreamToThree(
	tsvPath string,
	schema *arrow.Schema,
	mem memory.Allocator,
	studyID, profileID string,
) (<-chan arrow.RecordBatch, <-chan arrow.RecordBatch, <-chan arrow.RecordBatch, <-chan error) {
	alterationsRecordChan := make(chan arrow.RecordBatch, 10) // buffer a few records
	samplesRecordChan := make(chan arrow.RecordBatch, 10)     // buffer a few records
	derivedRecordChan := make(chan arrow.RecordBatch, 10)     // buffer a few records
	errChan := make(chan error, 1)

	go func() {
		defer close(alterationsRecordChan)
		defer close(samplesRecordChan)
		defer close(derivedRecordChan)
		defer close(errChan)

		rdr, file, err := ReadTSVAsRecords(tsvPath, schema, 100, '\t')
		if err != nil {
			errChan <- fmt.Errorf("failed to open TSV: %w", err)
			return
		}
		defer file.Close()
		defer rdr.Release()

		// Track if we've sent the samples record (should only send once)
		samplesSent := false

		for rdr.Next() {
			rec := rdr.RecordBatch()
			if rec == nil {
				continue
			}

			gar, gps, err := transformCNARecordBatch(studyID, profileID, rec, mem)
			if err != nil {
				rec.Release()
				errChan <- fmt.Errorf("transform failed: %w", err)
				return
			}

			derived, err := transformCNARecordBatchToDerived(studyID, profileID, rec, mem)
			rec.Release()

			if err != nil {
				gar.Release()
				gps.Release()
				errChan <- fmt.Errorf("transform to derived failed: %w", err)
				return
			}

			// Always send genetic alterations and derived records
			alterationsRecordChan <- gar
			derivedRecordChan <- derived

			// Only send genetic profile samples record on first batch
			if !samplesSent {
				samplesRecordChan <- gps
				samplesSent = true
			} else {
				// Release subsequent samples records since we don't need them
				gps.Release()
			}
		}

		if err := rdr.Err(); err != nil {
			errChan <- fmt.Errorf("TSV reader error: %w", err)
		}
	}()

	return alterationsRecordChan, samplesRecordChan, derivedRecordChan, errChan
}

// geneticAlterationsSchema defines the schema for the gene-centric record batch
var geneticAlterationsSchema = arrow.NewSchema([]arrow.Field{
	{Name: "CANCER_STUDY", Type: arrow.BinaryTypes.String},
	{Name: "GENETIC_PROFILE", Type: arrow.BinaryTypes.String},
	{Name: "GENE_SYMBOL", Type: arrow.BinaryTypes.String},
	{Name: "VALUES", Type: arrow.BinaryTypes.String},
}, nil)

// geneticProfileSamplesSchema defines the schema for the sample order record batch
var geneticProfileSamplesSchema = arrow.NewSchema([]arrow.Field{
	{Name: "CANCER_STUDY", Type: arrow.BinaryTypes.String},
	{Name: "GENETIC_PROFILE", Type: arrow.BinaryTypes.String},
	{Name: "ORDERED_SAMPLE_LIST", Type: arrow.BinaryTypes.String},
}, nil)

// cnaDerivedSchema defines the schema for the derived denormalized format
var cnaDerivedSchema = arrow.NewSchema([]arrow.Field{
	{Name: "SAMPLE_ID", Type: arrow.BinaryTypes.String},
	{Name: "CANCER_STUDY", Type: arrow.BinaryTypes.String},
	{Name: "GENE_SYMBOL", Type: arrow.BinaryTypes.String},
	{Name: "GENETIC_PROFILE", Type: arrow.BinaryTypes.String},
	{Name: "ALTERATION", Type: arrow.BinaryTypes.String},
}, nil)

// transformCNARecordBatch converts one tsv record batch into two output record batches:
// 1. A gene-centric batch with comma-delimited values per gene
// 2. A single-row batch containing the ordered sample list
func transformCNARecordBatch(
	cancerStudyName, geneticProfileName string,
	rec arrow.RecordBatch,
	mem memory.Allocator,
) (geneticAlterationsRec arrow.RecordBatch, geneticProfileSamplesRec arrow.RecordBatch, err error) {
	// Build the gene values record batch
	geneticAlterationsBuilder := array.NewRecordBuilder(mem, geneticAlterationsSchema)
	defer geneticAlterationsBuilder.Release()

	studyBldr := geneticAlterationsBuilder.Field(0).(*array.StringBuilder)
	profileBldr := geneticAlterationsBuilder.Field(1).(*array.StringBuilder)
	geneSymbolBldr := geneticAlterationsBuilder.Field(2).(*array.StringBuilder)
	valuesBldr := geneticAlterationsBuilder.Field(3).(*array.StringBuilder)

	// Build the sample order record batch
	geneticProfileSamplesBldr := array.NewRecordBuilder(mem, geneticProfileSamplesSchema)
	defer geneticProfileSamplesBldr.Release()

	sampleStudyBldr := geneticProfileSamplesBldr.Field(0).(*array.StringBuilder)
	sampleProfileBldr := geneticProfileSamplesBldr.Field(1).(*array.StringBuilder)
	sampleListBldr := geneticProfileSamplesBldr.Field(2).(*array.StringBuilder)

	// Get gene symbols column
	geneSymbols, ok := rec.Column(0).(*array.String)
	if !ok {
		return nil, nil, fmt.Errorf("first column (gene symbols) is not a string array")
	}

	prefix := cancerStudyName + "_"
	numCols := int(rec.NumCols())
	numRows := int(rec.NumRows())

	// Build the ordered sample list (we start at index 2 because 0:hugo, 1:entrez)
	var sampleIDs []string
	for colIdx := 2; colIdx < numCols; colIdx++ {
		colName := rec.ColumnName(colIdx)
		sampleID := prefix + colName
		sampleIDs = append(sampleIDs, sampleID)
	}
	orderedSampleList := strings.Join(sampleIDs, ",")

	// Add single row to sample order record batch
	sampleStudyBldr.Append(cancerStudyName)
	sampleProfileBldr.Append(geneticProfileName)
	sampleListBldr.Append(orderedSampleList)

	// Process each gene (row)
	for rowIdx := 0; rowIdx < numRows; rowIdx++ {
		geneSymbol := geneSymbols.Value(rowIdx)

		// Collect alteration values for this gene across all samples
		var alterationValues []string
		for colIdx := 2; colIdx < numCols; colIdx++ {
			alterations, ok := rec.Column(colIdx).(*array.String)
			if !ok {
				return nil, nil, fmt.Errorf("column %q is not a string array", rec.ColumnName(colIdx))
			}
			alterationValues = append(alterationValues, alterations.Value(rowIdx))
		}

		// Join the alteration values with commas
		valuesStr := strings.Join(alterationValues, ",")

		// Append to gene values record batch
		studyBldr.Append(cancerStudyName)
		profileBldr.Append(geneticProfileName)
		geneSymbolBldr.Append(geneSymbol)
		valuesBldr.Append(valuesStr)
	}

	// Create output record batches
	geneticAlterationsRec = geneticAlterationsBuilder.NewRecordBatch()
	geneticProfileSamplesRec = geneticProfileSamplesBldr.NewRecordBatch()

	// Validation for gene values record
	expectedGeneRows := geneticAlterationsRec.Column(0).Len()
	for i := 1; i < int(geneticAlterationsRec.NumCols()); i++ {
		col := geneticAlterationsRec.Column(i)
		if col.Len() != expectedGeneRows {
			geneticAlterationsRec.Release()
			geneticProfileSamplesRec.Release()
			return nil, nil, fmt.Errorf(
				"gene values column %q length mismatch: %d vs %d",
				geneticAlterationsRec.ColumnName(i), col.Len(), expectedGeneRows,
			)
		}
	}

	// Validation for sample order record (should have exactly 1 row)
	if geneticProfileSamplesRec.NumRows() != 1 {
		geneticAlterationsRec.Release()
		geneticProfileSamplesRec.Release()
		return nil, nil, fmt.Errorf(
			"sample order record should have 1 row, got %d",
			geneticProfileSamplesRec.NumRows(),
		)
	}

	return geneticAlterationsRec, geneticProfileSamplesRec, nil
}

// transformCNARecordBatchToDerived converts one tsv record batch into a derived denormalized format
// where each row represents one gene-sample-alteration combination
func transformCNARecordBatchToDerived(
	cancerStudyName, geneticProfileName string,
	rec arrow.RecordBatch,
	mem memory.Allocator,
) (arrow.RecordBatch, error) {
	bldr := array.NewRecordBuilder(mem, cnaDerivedSchema)
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
