package mutation

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

// MutationFileInput represents a single mutation file with its schema
type MutationFileInput struct {
	Path             string
	Schema           *arrow.Schema
	CancerStudyId    string
	GeneticProfileId string
}

// ProcessMultipleTSVToParquet processes multiple mutation TSV files and writes each to two parquet files:
// one for mutation events and one for mutations
func ProcessMultipleTSVToParquet(
	mutationFiles []MutationFileInput,
	outputDir string,
	mem memory.Allocator,
) error {
	// Track the mutation event ID across all TSV files to ensure uniqueness
	mutationEventID := int64(0)

	// Track failed files
	var failedFiles []string
	successCount := 0

	for _, mutationFile := range mutationFiles {
		log.Printf("processing: %s", mutationFile.Path)

		// generate output filenames based on input file path
		mutationEventPath, mutationPath := generateOutputPaths(mutationFile.Path, outputDir)

		newEventID, err := ProcessSingleTSVToTwoParquets(mutationFile, mutationEventPath, mutationPath, mutationEventID, mem)
		if err != nil {
			log.Printf("ERROR: failed to process %s: %v", mutationFile.Path, err)
			failedFiles = append(failedFiles, mutationFile.Path)
			// Continue processing remaining files instead of returning
			continue
		}

		// Update the mutation event ID for the next file
		mutationEventID = newEventID
		successCount++

		log.Printf("written: %s", mutationEventPath)
		log.Printf("written: %s", mutationPath)
	}

	// Print summary
	log.Printf("\n=== CONVERSION SUMMARY ===")
	log.Printf("Total files: %d", len(mutationFiles))
	log.Printf("Successfully converted: %d", successCount)
	log.Printf("Failed: %d", len(failedFiles))

	if len(failedFiles) > 0 {
		log.Printf("\nFailed files:")
		for _, path := range failedFiles {
			log.Printf("  - %s", path)
		}
		return fmt.Errorf("failed to convert %d out of %d files (see log for details)", len(failedFiles), len(mutationFiles))
	}

	return nil
}

func ProcessSingleTSVToTwoParquets(
	mutationFile MutationFileInput,
	mutationEventPath string,
	mutationPath string,
	startEventID int64,
	mem memory.Allocator,
) (int64, error) {
	mutationEventChan := make(chan arrow.RecordBatch, 10)
	mutationChan := make(chan arrow.RecordBatch, 10)

	// channel for all errors
	errChan := make(chan error, 4) // transform + 2 writers + forwarding

	// Channel to receive the final event ID from the transform goroutine
	finalEventIDChan := make(chan int64, 1)

	// start transform and forwarding
	go func() {
		defer close(mutationEventChan)
		defer close(mutationChan)

		mutationEventFileChan, mutationFileChan, fileErrChan, eventIDChan := transformTSVStreamToTwo(
			mutationFile.Path,
			mutationFile.Schema,
			mem,
			mutationFile.CancerStudyId,
			mutationFile.GeneticProfileId,
			startEventID,
		)

		var wg sync.WaitGroup
		wg.Add(2)

		// forward mutation events
		go func() {
			defer wg.Done()
			for rec := range mutationEventFileChan {
				mutationEventChan <- rec
			}
		}()

		// forward mutations
		go func() {
			defer wg.Done()
			for rec := range mutationFileChan {
				mutationChan <- rec
			}
		}()

		wg.Wait()

		// check for transform errors and get final event ID
		if err := <-fileErrChan; err != nil {
			errChan <- fmt.Errorf("error processing %s: %w", mutationFile.Path, err)
		}
		finalEventIDChan <- <-eventIDChan
	}()

	// write both parquet files concurrently
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		if err := WriteParquetStream(mutationEventPath, mutationEventChan, mem); err != nil {
			errChan <- err
		}
	}()

	go func() {
		defer wg.Done()
		if err := WriteParquetStream(mutationPath, mutationChan, mem); err != nil {
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
		return 0, fmt.Errorf("errors: %v", errs)
	}

	// Get the final event ID
	finalEventID := <-finalEventIDChan

	return finalEventID, nil
}

// generateOutputPaths creates two output paths: one for mutation events and one for mutations
func generateOutputPaths(inputPath, outputDir string) (mutationEventPath, mutationPath string) {
	// get the study dir name
	dir := filepath.Base(filepath.Dir(inputPath))
	// get the base filename without extension
	base := filepath.Base(inputPath)
	// remove .txt extension
	nameWithoutExt := strings.TrimSuffix(base, filepath.Ext(base))

	// create two different output filenames
	mutationEventFilename := dir + "_" + nameWithoutExt + "_mutation_event.parquet"
	mutationFilename := dir + "_" + nameWithoutExt + "_mutation.parquet"

	return filepath.Join(outputDir, mutationEventFilename), filepath.Join(outputDir, mutationFilename)
}

// transformTSVStreamToTwo reads records from TSV and transforms them into two arrow record batch streams:
// one for mutation events and one for mutations
func transformTSVStreamToTwo(
	tsvPath string,
	schema *arrow.Schema,
	mem memory.Allocator,
	studyID, profileID string,
	startEventID int64,
) (<-chan arrow.RecordBatch, <-chan arrow.RecordBatch, <-chan error, <-chan int64) {
	mutationEventRecordChan := make(chan arrow.RecordBatch, 10) // buffer a few records
	mutationRecordChan := make(chan arrow.RecordBatch, 10)      // buffer a few records
	errChan := make(chan error, 1)
	eventIDChan := make(chan int64, 1)

	go func() {
		defer close(mutationEventRecordChan)
		defer close(mutationRecordChan)
		defer close(errChan)
		defer close(eventIDChan)

		rdr, file, err := ReadTSVAsRecords(tsvPath, schema, 100, '\t')
		if err != nil {
			errChan <- fmt.Errorf("failed to open TSV: %w", err)
			eventIDChan <- startEventID
			return
		}
		defer file.Close()
		defer rdr.Release()

		// Track the current MUTATION_EVENT_ID (starts from provided value)
		mutationEventID := startEventID

		for rdr.Next() {
			rec := rdr.RecordBatch()
			if rec == nil {
				continue
			}

			mutationEventRec, mutationRec, newEventID, err := transformMutationRecordBatch(studyID, profileID, rec, mutationEventID, mem)
			rec.Release()

			if err != nil {
				errChan <- fmt.Errorf("transform failed: %w", err)
				return
			}

			// Update the mutation event ID for the next batch
			mutationEventID = newEventID

			// Send both records
			mutationEventRecordChan <- mutationEventRec
			mutationRecordChan <- mutationRec
		}

		if err := rdr.Err(); err != nil {
			errChan <- fmt.Errorf("TSV reader error: %w", err)
		}

		// Send the final event ID
		eventIDChan <- mutationEventID
	}()

	return mutationEventRecordChan, mutationRecordChan, errChan, eventIDChan
}

// mutationEventSchema defines the schema for the mutation_event table
var mutationEventSchema = arrow.NewSchema([]arrow.Field{
	{Name: "MUTATION_EVENT_ID", Type: arrow.PrimitiveTypes.Int64},
	{Name: "ENTREZ_GENE_ID", Type: arrow.BinaryTypes.String},
	{Name: "CHR", Type: arrow.BinaryTypes.String},
	{Name: "START_POSITION", Type: arrow.BinaryTypes.String},
	{Name: "END_POSITION", Type: arrow.BinaryTypes.String},
	{Name: "REFERENCE_ALLELE", Type: arrow.BinaryTypes.String},
	{Name: "TUMOR_SEQ_ALLELE", Type: arrow.BinaryTypes.String},
	{Name: "PROTEIN_CHANGE", Type: arrow.BinaryTypes.String},
	{Name: "MUTATION_TYPE", Type: arrow.BinaryTypes.String},
	{Name: "NCBI_BUILD", Type: arrow.BinaryTypes.String},
	{Name: "STRAND", Type: arrow.BinaryTypes.String},
	{Name: "VARIANT_TYPE", Type: arrow.BinaryTypes.String},
	{Name: "DB_SNP_RS", Type: arrow.BinaryTypes.String},
	{Name: "DB_SNP_VAL_STATUS", Type: arrow.BinaryTypes.String},
	{Name: "REFSEQ_MRNA_ID", Type: arrow.BinaryTypes.String},
	{Name: "CODON_CHANGE", Type: arrow.BinaryTypes.String},
	{Name: "UNIPROT_ACCESSION", Type: arrow.BinaryTypes.String},
	{Name: "PROTEIN_POS_START", Type: arrow.BinaryTypes.String},
	{Name: "PROTEIN_POS_END", Type: arrow.BinaryTypes.String},
	{Name: "CANONICAL_TRANSCRIPT", Type: arrow.BinaryTypes.String},
	{Name: "KEYWORD", Type: arrow.BinaryTypes.String},
}, nil)

// mutationSchema defines the schema for the mutation table
var mutationSchema = arrow.NewSchema([]arrow.Field{
	{Name: "MUTATION_EVENT_ID", Type: arrow.PrimitiveTypes.Int64},
	{Name: "GENETIC_PROFILE_ID", Type: arrow.BinaryTypes.String},
	{Name: "SAMPLE_ID", Type: arrow.BinaryTypes.String},
	{Name: "ENTREZ_GENE_ID", Type: arrow.BinaryTypes.String},
	{Name: "CENTER", Type: arrow.BinaryTypes.String},
	{Name: "SEQUENCER", Type: arrow.BinaryTypes.String},
	{Name: "MUTATION_STATUS", Type: arrow.BinaryTypes.String},
	{Name: "VALIDATION_STATUS", Type: arrow.BinaryTypes.String},
	{Name: "TUMOR_SEQ_ALLELE1", Type: arrow.BinaryTypes.String},
	{Name: "TUMOR_SEQ_ALLELE2", Type: arrow.BinaryTypes.String},
	{Name: "MATCHED_NORM_SAMPLE_BARCODE", Type: arrow.BinaryTypes.String},
	{Name: "MATCH_NORM_SEQ_ALLELE1", Type: arrow.BinaryTypes.String},
	{Name: "MATCH_NORM_SEQ_ALLELE2", Type: arrow.BinaryTypes.String},
	{Name: "TUMOR_VALIDATION_ALLELE1", Type: arrow.BinaryTypes.String},
	{Name: "TUMOR_VALIDATION_ALLELE2", Type: arrow.BinaryTypes.String},
	{Name: "MATCH_NORM_VALIDATION_ALLELE1", Type: arrow.BinaryTypes.String},
	{Name: "MATCH_NORM_VALIDATION_ALLELE2", Type: arrow.BinaryTypes.String},
	{Name: "VERIFICATION_STATUS", Type: arrow.BinaryTypes.String},
	{Name: "SEQUENCING_PHASE", Type: arrow.BinaryTypes.String},
	{Name: "SEQUENCE_SOURCE", Type: arrow.BinaryTypes.String},
	{Name: "VALIDATION_METHOD", Type: arrow.BinaryTypes.String},
	{Name: "SCORE", Type: arrow.BinaryTypes.String},
	{Name: "BAM_FILE", Type: arrow.BinaryTypes.String},
	{Name: "TUMOR_ALT_COUNT", Type: arrow.BinaryTypes.String},
	{Name: "TUMOR_REF_COUNT", Type: arrow.BinaryTypes.String},
	{Name: "NORMAL_ALT_COUNT", Type: arrow.BinaryTypes.String},
	{Name: "NORMAL_REF_COUNT", Type: arrow.BinaryTypes.String},
	{Name: "AMINO_ACID_CHANGE", Type: arrow.BinaryTypes.String},
	{Name: "ANNOTATION_JSON", Type: arrow.BinaryTypes.String},
}, nil)

// Helper function to get field value from record batch by field name
func getFieldValue(rec arrow.RecordBatch, fieldName string, rowIdx int) string {
	for i := 0; i < int(rec.NumCols()); i++ {
		if rec.ColumnName(i) == fieldName {
			col, ok := rec.Column(i).(*array.String)
			if !ok {
				return ""
			}
			if rowIdx < col.Len() {
				return col.Value(rowIdx)
			}
		}
	}
	return ""
}

// transformMutationRecordBatch converts one TSV record batch into two output record batches:
// 1. A mutation_event batch with one row per unique mutation event
// 2. A mutation batch with one row per mutation linking to the event
func transformMutationRecordBatch(
	cancerStudyName, geneticProfileName string,
	rec arrow.RecordBatch,
	startEventID int64,
	mem memory.Allocator,
) (mutationEventRec arrow.RecordBatch, mutationRec arrow.RecordBatch, nextEventID int64, err error) {
	// Build the mutation_event record batch
	mutationEventBuilder := array.NewRecordBuilder(mem, mutationEventSchema)
	defer mutationEventBuilder.Release()

	// Build the mutation record batch
	mutationBuilder := array.NewRecordBuilder(mem, mutationSchema)
	defer mutationBuilder.Release()

	numRows := int(rec.NumRows())
	currentEventID := startEventID

	// Process each row in the input batch
	for rowIdx := 0; rowIdx < numRows; rowIdx++ {
		// === Build mutation_event row ===
		mutationEventBuilder.Field(0).(*array.Int64Builder).Append(currentEventID)
		mutationEventBuilder.Field(1).(*array.StringBuilder).Append(getFieldValue(rec, "Entrez_Gene_Id", rowIdx))
		mutationEventBuilder.Field(2).(*array.StringBuilder).Append(getFieldValue(rec, "Chromosome", rowIdx))
		mutationEventBuilder.Field(3).(*array.StringBuilder).Append(getFieldValue(rec, "Start_Position", rowIdx))
		mutationEventBuilder.Field(4).(*array.StringBuilder).Append(getFieldValue(rec, "End_Position", rowIdx))
		mutationEventBuilder.Field(5).(*array.StringBuilder).Append(getFieldValue(rec, "Reference_Allele", rowIdx))
		mutationEventBuilder.Field(6).(*array.StringBuilder).Append(getFieldValue(rec, "Tumor_Seq_Allele2", rowIdx))
		mutationEventBuilder.Field(7).(*array.StringBuilder).Append(getFieldValue(rec, "HGVSp_Short", rowIdx))
		mutationEventBuilder.Field(8).(*array.StringBuilder).Append(getFieldValue(rec, "Variant_Classification", rowIdx))
		mutationEventBuilder.Field(9).(*array.StringBuilder).Append(getFieldValue(rec, "NCBI_Build", rowIdx))
		mutationEventBuilder.Field(10).(*array.StringBuilder).Append(getFieldValue(rec, "Strand", rowIdx))
		mutationEventBuilder.Field(11).(*array.StringBuilder).Append(getFieldValue(rec, "Variant_Type", rowIdx))
		mutationEventBuilder.Field(12).(*array.StringBuilder).Append(getFieldValue(rec, "dbSNP_RS", rowIdx))
		mutationEventBuilder.Field(13).(*array.StringBuilder).Append(getFieldValue(rec, "dbSNP_Val_Status", rowIdx))
		// Fields 14-20 don't have corresponding MAF fields, so we'll leave them empty
		mutationEventBuilder.Field(14).(*array.StringBuilder).Append("")
		mutationEventBuilder.Field(15).(*array.StringBuilder).Append("")
		mutationEventBuilder.Field(16).(*array.StringBuilder).Append("")
		mutationEventBuilder.Field(17).(*array.StringBuilder).Append("")
		mutationEventBuilder.Field(18).(*array.StringBuilder).Append("")
		mutationEventBuilder.Field(19).(*array.StringBuilder).Append("")
		mutationEventBuilder.Field(20).(*array.StringBuilder).Append("")

		// === Build mutation row ===
		mutationBuilder.Field(0).(*array.Int64Builder).Append(currentEventID)
		mutationBuilder.Field(1).(*array.StringBuilder).Append(geneticProfileName)

		// For SAMPLE_ID, prepend the cancer study name to the tumor sample barcode
		tumorSampleBarcode := getFieldValue(rec, "Tumor_Sample_Barcode", rowIdx)
		sampleID := cancerStudyName + "_" + tumorSampleBarcode
		mutationBuilder.Field(2).(*array.StringBuilder).Append(sampleID)

		mutationBuilder.Field(3).(*array.StringBuilder).Append(getFieldValue(rec, "Entrez_Gene_Id", rowIdx))
		mutationBuilder.Field(4).(*array.StringBuilder).Append(getFieldValue(rec, "Center", rowIdx))
		mutationBuilder.Field(5).(*array.StringBuilder).Append(getFieldValue(rec, "Sequencer", rowIdx))
		mutationBuilder.Field(6).(*array.StringBuilder).Append(getFieldValue(rec, "Mutation_Status", rowIdx))
		mutationBuilder.Field(7).(*array.StringBuilder).Append(getFieldValue(rec, "Validation_Status", rowIdx))
		mutationBuilder.Field(8).(*array.StringBuilder).Append(getFieldValue(rec, "Tumor_Seq_Allele1", rowIdx))
		mutationBuilder.Field(9).(*array.StringBuilder).Append(getFieldValue(rec, "Tumor_Seq_Allele2", rowIdx))
		mutationBuilder.Field(10).(*array.StringBuilder).Append(getFieldValue(rec, "Matched_Norm_Sample_Barcode", rowIdx))
		mutationBuilder.Field(11).(*array.StringBuilder).Append(getFieldValue(rec, "Match_Norm_Seq_Allele1", rowIdx))
		mutationBuilder.Field(12).(*array.StringBuilder).Append(getFieldValue(rec, "Match_Norm_Seq_Allele2", rowIdx))
		mutationBuilder.Field(13).(*array.StringBuilder).Append(getFieldValue(rec, "Tumor_Validation_Allele1", rowIdx))
		mutationBuilder.Field(14).(*array.StringBuilder).Append(getFieldValue(rec, "Tumor_Validation_Allele2", rowIdx))
		mutationBuilder.Field(15).(*array.StringBuilder).Append(getFieldValue(rec, "Match_Norm_Validation_Allele1", rowIdx))
		mutationBuilder.Field(16).(*array.StringBuilder).Append(getFieldValue(rec, "Match_Norm_Validation_Allele2", rowIdx))
		mutationBuilder.Field(17).(*array.StringBuilder).Append(getFieldValue(rec, "Verification_Status", rowIdx))
		mutationBuilder.Field(18).(*array.StringBuilder).Append(getFieldValue(rec, "Sequencing_Phase", rowIdx))
		mutationBuilder.Field(19).(*array.StringBuilder).Append(getFieldValue(rec, "Sequence_Source", rowIdx))
		mutationBuilder.Field(20).(*array.StringBuilder).Append(getFieldValue(rec, "Validation_Method", rowIdx))
		mutationBuilder.Field(21).(*array.StringBuilder).Append(getFieldValue(rec, "Score", rowIdx))
		mutationBuilder.Field(22).(*array.StringBuilder).Append(getFieldValue(rec, "BAM_File", rowIdx))
		mutationBuilder.Field(23).(*array.StringBuilder).Append(getFieldValue(rec, "t_alt_count", rowIdx))
		mutationBuilder.Field(24).(*array.StringBuilder).Append(getFieldValue(rec, "t_ref_count", rowIdx))
		mutationBuilder.Field(25).(*array.StringBuilder).Append(getFieldValue(rec, "n_alt_count", rowIdx))
		mutationBuilder.Field(26).(*array.StringBuilder).Append(getFieldValue(rec, "n_ref_count", rowIdx))
		mutationBuilder.Field(27).(*array.StringBuilder).Append(getFieldValue(rec, "HGVSp_Short", rowIdx))
		mutationBuilder.Field(28).(*array.StringBuilder).Append("") // ANNOTATION_JSON - empty for now

		// Increment the mutation event ID for the next row
		currentEventID++
	}

	// Create output record batches
	mutationEventRec = mutationEventBuilder.NewRecordBatch()
	mutationRec = mutationBuilder.NewRecordBatch()

	return mutationEventRec, mutationRec, currentEventID, nil
}
