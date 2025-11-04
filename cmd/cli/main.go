package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/cbioportal/clickhouse-only-importer-prototype/cna"
	"github.com/cbioportal/clickhouse-only-importer-prototype/mutation"
)

// example workflows:

// convert-cna mode:
// Converts CNA TSV files to two parquet files each: *_genetic_alterations.parquet and *_genetic_profile_samples.parquet
//go run ./cmd/cli/main.go -mode convert-cna -tsv-dir ./data -parquet-dir ./output

// convert-cna-with-derived mode:
// Converts CNA TSV files to three parquet files each: *_genetic_alterations.parquet, *_genetic_profile_samples.parquet, and *_derived.parquet
//go run ./cmd/cli/main.go -mode convert-cna-with-derived -tsv-dir ./data -parquet-dir ./output

// convert-mutations mode:
// Converts mutation TSV files to two parquet files each: *_mutation_event.parquet and *_mutation.parquet
//go run ./cmd/cli/main.go -mode convert-mutations -tsv-dir ./data -parquet-dir ./output

// combine-cna mode:
// Combines CNA parquet files into two final files: combined-all-cna_genetic_alterations.parquet and combined-all-cna_genetic_profile_samples.parquet
//go run ./cmd/cli/main.go -mode combine-cna -parquet-dir ./output -output combined-all-cna.parquet

// combine-cna-with-derived mode:
// Combines CNA parquet files into three final files: *_genetic_alterations.parquet, *_genetic_profile_samples.parquet, and *_derived.parquet
//go run ./cmd/cli/main.go -mode combine-cna-with-derived -parquet-dir ./output -output combined-all-cna.parquet

// combine-mutations mode:
// Combines mutation parquet files into two final files: *_mutation_event.parquet and *_mutation.parquet
//go run ./cmd/cli/main.go -mode combine-mutations -parquet-dir ./output -output combined-all-mutations.parquet

func main() {
	// command-line flags
	mode := flag.String(
		"mode",
		"convert-cna",
		"Operation mode: 'convert-cna' (CNA TSV to parquet), 'convert-cna-with-derived' (CNA TSV to parquet with derived), 'convert-mutations' (mutation TSV to parquet), 'combine-cna' (merge CNA parquet files), 'combine-cna-with-derived' (merge CNA parquet files with derived), or 'combine-mutations' (merge mutation parquet files)",
	)
	tsvRootDir := flag.String("tsv-dir", "", "Root directory containing TSV files (convert modes)")
	parquetDir := flag.String(
		"parquet-dir",
		"",
		"Directory for parquet files (input for combine modes, output for convert modes)",
	)
	outputFile := flag.String(
		"output",
		"combined-all-cna.parquet",
		"Output filename for combined parquet (combine modes)",
	)
	flag.Parse()

	// validate mode
	if *mode != "convert-cna" && *mode != "convert-cna-with-derived" && *mode != "convert-mutations" && *mode != "combine-cna" && *mode != "combine-cna-with-derived" && *mode != "combine-mutations" {
		log.Fatal("Invalid mode. Must be 'convert-cna', 'convert-cna-with-derived', 'convert-mutations', 'combine-cna', 'combine-cna-with-derived', or 'combine-mutations'")
	}

	// validate required flags
	if *parquetDir == "" {
		log.Fatal("Error: -parquet-dir is required")
	}

	mem := memory.DefaultAllocator
	startTime := time.Now()

	switch *mode {
	case "convert-cna":
		if *tsvRootDir == "" {
			log.Fatal("Error: -tsv-dir is required for convert-cna mode")
		}
		runCNAConvertMode(*tsvRootDir, *parquetDir, false, mem)

	case "convert-cna-with-derived":
		if *tsvRootDir == "" {
			log.Fatal("Error: -tsv-dir is required for convert-cna-with-derived mode")
		}
		runCNAConvertMode(*tsvRootDir, *parquetDir, true, mem)

	case "convert-mutations":
		if *tsvRootDir == "" {
			log.Fatal("Error: -tsv-dir is required for convert-mutations mode")
		}
		runMutationConvertMode(*tsvRootDir, *parquetDir, mem)

	case "combine-cna":
		runCNACombineMode(*parquetDir, *outputFile, false, mem)

	case "combine-cna-with-derived":
		runCNACombineMode(*parquetDir, *outputFile, true, mem)

	case "combine-mutations":
		runMutationCombineMode(*parquetDir, *outputFile, mem)
	}

	elapsed := time.Since(startTime)
	log.Printf("Total execution time: %s", elapsed)
}

func runCNAConvertMode(tsvRootDir, parquetDir string, includeDerived bool, mem memory.Allocator) {
	if includeDerived {
		log.Print("=== CONVERT CNA WITH DERIVED MODE: TSV to individual Parquet files (with derived) ===")
	} else {
		log.Print("=== CONVERT CNA MODE: TSV to individual Parquet files ===")
	}
	log.Printf("Scanning for meta_cna.txt files in: %s", tsvRootDir)

	cnaFiles, err := findCNAFiles(tsvRootDir)
	if err != nil {
		log.Fatalf("Error finding CNA files: %v", err)
	}

	if len(cnaFiles) == 0 {
		log.Printf("No data_CNA.txt files found in %s", tsvRootDir)
		return
	}

	log.Printf("Found %d CNA files", len(cnaFiles))

	// Create output directory if it doesn't exist
	if err := os.MkdirAll(parquetDir, 0755); err != nil {
		log.Fatalf("Failed to create output directory: %v", err)
	}

	if includeDerived {
		log.Print("Transforming TSV files and writing individual parquet files including derived (streaming)")
		if err := cna.ProcessMultipleTSVToParquetWithDerived(cnaFiles, parquetDir, mem); err != nil {
			log.Fatalf("Error processing TSV files: %v", err)
		}
		log.Printf("✓ Individual parquet files (including derived) written to: %s", parquetDir)
	} else {
		log.Print("Transforming TSV files and writing individual parquet files (streaming)")
		if err := cna.ProcessMultipleTSVToParquet(cnaFiles, parquetDir, mem); err != nil {
			log.Fatalf("Error processing TSV files: %v", err)
		}
		log.Printf("✓ Individual parquet files written to: %s", parquetDir)
	}

	log.Printf("✓ Number of CNA files processed: %d", len(cnaFiles))
}

func runCNACombineMode(parquetDir, outputFile string, includeDerived bool, mem memory.Allocator) {
	if includeDerived {
		log.Print("=== COMBINE CNA WITH DERIVED MODE: Merge Parquet files into three combined files ===")
	} else {
		log.Print("=== COMBINE CNA MODE: Merge Parquet files into two combined files ===")
	}
	log.Printf("Reading parquet files from: %s", parquetDir)

	var alterationsOutputPath, samplesOutputPath, derivedOutputPath string

	if includeDerived {
		alterationsOutputPath, samplesOutputPath, derivedOutputPath = generateCombinedOutputPathsWithDerived(parquetDir, outputFile)
		log.Printf("Combining genetic alterations files into: %s", alterationsOutputPath)
		log.Printf("Combining genetic profile samples files into: %s", samplesOutputPath)
		log.Printf("Combining derived files into: %s", derivedOutputPath)
	} else {
		alterationsOutputPath, samplesOutputPath = generateCombinedOutputPaths(parquetDir, outputFile)
		log.Printf("Combining genetic alterations files into: %s", alterationsOutputPath)
		log.Printf("Combining genetic profile samples files into: %s", samplesOutputPath)
	}

	// Combine genetic alterations files
	alterationsPattern := filepath.Join(parquetDir, "*_genetic_alterations.parquet")
	if err := cna.CombineParquetFilesByPattern(alterationsPattern, alterationsOutputPath, mem); err != nil {
		log.Fatalf("Error combining genetic alterations files: %v", err)
	}
	log.Printf("✓ Combined genetic alterations file written to: %s", alterationsOutputPath)

	// Combine genetic profile samples files
	samplesPattern := filepath.Join(parquetDir, "*_genetic_profile_samples.parquet")
	if err := cna.CombineParquetFilesByPattern(samplesPattern, samplesOutputPath, mem); err != nil {
		log.Fatalf("Error combining genetic profile samples files: %v", err)
	}
	log.Printf("✓ Combined genetic profile samples file written to: %s", samplesOutputPath)

	// Combine derived files if requested
	if includeDerived {
		derivedPattern := filepath.Join(parquetDir, "*_derived.parquet")
		if err := cna.CombineParquetFilesByPattern(derivedPattern, derivedOutputPath, mem); err != nil {
			log.Fatalf("Error combining derived files: %v", err)
		}
		log.Printf("✓ Combined derived file written to: %s", derivedOutputPath)
	}
}

// generateCombinedOutputPaths creates two output paths for combined files based on a base output filename
func generateCombinedOutputPaths(parquetDir, baseOutputFile string) (alterationsPath, samplesPath string) {
	// Use absolute path if provided, otherwise join with parquet directory
	var basePath string
	if filepath.IsAbs(baseOutputFile) {
		basePath = baseOutputFile
	} else {
		basePath = filepath.Join(parquetDir, baseOutputFile)
	}

	// Remove .parquet extension if present
	basePathWithoutExt := strings.TrimSuffix(basePath, ".parquet")

	// Create two output paths
	alterationsPath = basePathWithoutExt + "_genetic_alterations.parquet"
	samplesPath = basePathWithoutExt + "_genetic_profile_samples.parquet"

	return alterationsPath, samplesPath
}

// generateCombinedOutputPathsWithDerived creates three output paths for combined files including derived
func generateCombinedOutputPathsWithDerived(parquetDir, baseOutputFile string) (alterationsPath, samplesPath, derivedPath string) {
	// Use absolute path if provided, otherwise join with parquet directory
	var basePath string
	if filepath.IsAbs(baseOutputFile) {
		basePath = baseOutputFile
	} else {
		basePath = filepath.Join(parquetDir, baseOutputFile)
	}

	// Remove .parquet extension if present
	basePathWithoutExt := strings.TrimSuffix(basePath, ".parquet")

	// Create three output paths
	alterationsPath = basePathWithoutExt + "_genetic_alterations.parquet"
	samplesPath = basePathWithoutExt + "_genetic_profile_samples.parquet"
	derivedPath = basePathWithoutExt + "_derived.parquet"

	return alterationsPath, samplesPath, derivedPath
}

func findCNAFiles(rootDir string) ([]cna.CNAFileInput, error) {
	var cnaFiles []cna.CNAFileInput
	// Maps to track meta files by directory and their data_filename
	type metaInfo struct {
		path          string
		cancerStudyId string
		stableId      string
	}
	// Key: directory + data_filename (e.g., "/path/to/study/data_cna.txt")
	metaFilesByDataFile := make(map[string]*metaInfo)
	dataFilesByDir := make(map[string][]string)

	// first pass: collect all meta and data files
	err := filepath.Walk(rootDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		// Skip directories that contain "case_lists" in the path
		if strings.Contains(path, "case_lists") {
			if info.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		if info.IsDir() {
			return nil
		}

		fileName := strings.ToLower(info.Name())
		dir := filepath.Dir(path)

		// Check for meta file
		metaMatched, _ := regexp.MatchString(`^meta_.*cna.*\.txt$`, fileName)
		if metaMatched && !strings.Contains(fileName, "seg") {
			// Extract metadata including data_filename
			cancerStudyId, stableId, dataFilename, err := extractMetadataWithDataFilename(path)
			if err != nil {
				log.Printf("warning: failed to extract metadata from %s: %v", path, err)
				return nil
			}

			if dataFilename == "" {
				log.Printf("warning: meta file %s has no data_filename property", path)
				return nil
			}

			// Store meta info indexed by full path to data file
			dataFilePath := filepath.Join(dir, dataFilename)
			metaFilesByDataFile[dataFilePath] = &metaInfo{
				path:          path,
				cancerStudyId: cancerStudyId,
				stableId:      stableId,
			}
			log.Printf("meta file %s references data file %s", path, dataFilePath)
			return nil
		}

		// Check for data file (skip files with "seg" in the name)
		dataMatched, _ := regexp.MatchString(`^data_.*cna.*\.txt$`, fileName)
		if dataMatched && !strings.Contains(fileName, "seg") {
			dataFilesByDir[dir] = append(dataFilesByDir[dir], path)
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	// second pass: process data files with their paired meta files
	for _, dataPaths := range dataFilesByDir {
		for _, path := range dataPaths {
			log.Printf("found: %s", path)

			// Get schema for this file
			f, err := os.Open(path)
			if err != nil {
				return nil, fmt.Errorf("failed to open %s: %w", path, err)
			}
			schema, err := cna.GetFieldTypesFromFile(f, '\t')
			f.Close()
			if err != nil {
				return nil, fmt.Errorf("failed to get schema from %s: %w", path, err)
			}

			// Look for paired meta file using the data file path
			var cancerStudyId, stableId string
			if meta, hasMeta := metaFilesByDataFile[path]; hasMeta {
				cancerStudyId = meta.cancerStudyId
				stableId = meta.stableId
				log.Printf("matched meta file %s for data file %s", meta.path, path)
			} else {
				log.Printf("warning: no meta file found for %s", path)
			}

			geneticProfileId := ""
			if cancerStudyId != "" && stableId != "" {
				geneticProfileId = cancerStudyId + "_" + stableId
			}

			cnaFiles = append(cnaFiles, cna.CNAFileInput{
				Path:             path,
				Schema:           schema,
				CancerStudyId:    cancerStudyId,
				GeneticProfileId: geneticProfileId,
			})
		}
	}

	return cnaFiles, nil
}

// extractMetadataWithDataFilename reads a meta file and extracts cancer_study_identifier, stable_id, and data_filename
func extractMetadataWithDataFilename(metaPath string) (cancerStudyId, stableId, dataFilename string, err error) {
	file, err := os.Open(metaPath)
	if err != nil {
		return "", "", "", err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		parts := strings.SplitN(line, ":", 2)
		if len(parts) != 2 {
			continue
		}

		key := strings.TrimSpace(parts[0])
		value := strings.TrimSpace(parts[1])

		switch key {
		case "cancer_study_identifier":
			cancerStudyId = value
		case "stable_id":
			stableId = value
		case "data_filename":
			dataFilename = value
		}

		// Early exit if we found all three
		if cancerStudyId != "" && stableId != "" && dataFilename != "" {
			break
		}
	}

	if err := scanner.Err(); err != nil {
		return "", "", "", err
	}

	return cancerStudyId, stableId, dataFilename, nil
}

func runMutationConvertMode(tsvRootDir, parquetDir string, mem memory.Allocator) {
	log.Print("=== CONVERT MUTATIONS MODE: TSV to individual Parquet files ===")
	log.Printf("Scanning for meta_mutations.txt files in: %s", tsvRootDir)

	mutationFiles, err := findMutationFiles(tsvRootDir)
	if err != nil {
		log.Fatalf("Error finding mutation files: %v", err)
	}

	if len(mutationFiles) == 0 {
		log.Printf("No mutation files found in %s", tsvRootDir)
		return
	}

	log.Printf("Found %d mutation files", len(mutationFiles))

	// Create output directory if it doesn't exist
	if err := os.MkdirAll(parquetDir, 0755); err != nil {
		log.Fatalf("Failed to create output directory: %v", err)
	}

	log.Print("Transforming mutation TSV files and writing individual parquet files (streaming)")
	if err := mutation.ProcessMultipleTSVToParquet(mutationFiles, parquetDir, mem); err != nil {
		log.Fatalf("Error processing mutation TSV files: %v", err)
	}
	log.Printf("✓ Individual parquet files written to: %s", parquetDir)

	log.Printf("✓ Number of mutation files processed: %d", len(mutationFiles))
}

func findMutationFiles(rootDir string) ([]mutation.MutationFileInput, error) {
	var mutationFiles []mutation.MutationFileInput
	// Maps to track meta files by directory and their data_filename
	type metaInfo struct {
		path          string
		cancerStudyId string
		stableId      string
	}
	// Key: directory + data_filename (e.g., "/path/to/study/data_mutations.txt")
	metaFilesByDataFile := make(map[string]*metaInfo)
	dataFilesByDir := make(map[string][]string)

	// first pass: collect all meta and data files
	err := filepath.Walk(rootDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		// Skip directories that contain "case_lists" in the path
		if strings.Contains(path, "case_lists") {
			if info.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		if info.IsDir() {
			return nil
		}

		fileName := strings.ToLower(info.Name())
		dir := filepath.Dir(path)

		// Check for meta file (specifically meta_mutations.txt or meta_mutations_*.txt)
		metaMatched, _ := regexp.MatchString(`^meta_mutations.*\.txt$`, fileName)
		if metaMatched {
			// Extract metadata including data_filename
			cancerStudyId, stableId, dataFilename, err := extractMetadataWithDataFilename(path)
			if err != nil {
				log.Printf("warning: failed to extract metadata from %s: %v", path, err)
				return nil
			}

			if dataFilename == "" {
				log.Printf("warning: meta file %s has no data_filename property", path)
				return nil
			}

			// Store meta info indexed by full path to data file
			dataFilePath := filepath.Join(dir, dataFilename)
			metaFilesByDataFile[dataFilePath] = &metaInfo{
				path:          path,
				cancerStudyId: cancerStudyId,
				stableId:      stableId,
			}
			log.Printf("meta file %s references data file %s", path, dataFilePath)
			return nil
		}

		// Check for data file (mutation data files typically match data_mutations*.txt pattern)
		dataMatched, _ := regexp.MatchString(`^data_mutations.*\.txt$`, fileName)
		if dataMatched {
			dataFilesByDir[dir] = append(dataFilesByDir[dir], path)
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	// second pass: process data files with their paired meta files
	for _, dataPaths := range dataFilesByDir {
		for _, path := range dataPaths {
			log.Printf("found: %s", path)

			// Get schema for this file
			f, err := os.Open(path)
			if err != nil {
				return nil, fmt.Errorf("failed to open %s: %w", path, err)
			}
			schema, err := mutation.GetFieldTypesFromFile(f, '\t')
			f.Close()
			if err != nil {
				return nil, fmt.Errorf("failed to get schema from %s: %w", path, err)
			}

			// Look for paired meta file using the data file path
			var cancerStudyId, stableId string
			if meta, hasMeta := metaFilesByDataFile[path]; hasMeta {
				cancerStudyId = meta.cancerStudyId
				stableId = meta.stableId
				log.Printf("matched meta file %s for data file %s", meta.path, path)
			} else {
				log.Printf("warning: no meta file found for %s", path)
			}

			geneticProfileId := ""
			if cancerStudyId != "" && stableId != "" {
				geneticProfileId = cancerStudyId + "_" + stableId
			}

			mutationFiles = append(mutationFiles, mutation.MutationFileInput{
				Path:             path,
				Schema:           schema,
				CancerStudyId:    cancerStudyId,
				GeneticProfileId: geneticProfileId,
			})
		}
	}

	return mutationFiles, nil
}

func runMutationCombineMode(parquetDir, outputFile string, mem memory.Allocator) {
	log.Print("=== COMBINE MUTATIONS MODE: Merge Parquet files into two combined files ===")
	log.Printf("Reading parquet files from: %s", parquetDir)

	mutationEventOutputPath, mutationOutputPath := generateCombinedMutationOutputPaths(parquetDir, outputFile)
	log.Printf("Combining mutation event files into: %s", mutationEventOutputPath)
	log.Printf("Combining mutation files into: %s", mutationOutputPath)

	// Combine mutation_event files
	mutationEventPattern := filepath.Join(parquetDir, "*_mutation_event.parquet")
	if err := mutation.CombineParquetFilesByPattern(mutationEventPattern, mutationEventOutputPath, mem); err != nil {
		log.Fatalf("Error combining mutation event files: %v", err)
	}
	log.Printf("✓ Combined mutation event file written to: %s", mutationEventOutputPath)

	// Combine mutation files
	mutationPattern := filepath.Join(parquetDir, "*_mutation.parquet")
	if err := mutation.CombineParquetFilesByPattern(mutationPattern, mutationOutputPath, mem); err != nil {
		log.Fatalf("Error combining mutation files: %v", err)
	}
	log.Printf("✓ Combined mutation file written to: %s", mutationOutputPath)
}

// generateCombinedMutationOutputPaths creates two output paths for combined mutation files based on a base output filename
func generateCombinedMutationOutputPaths(parquetDir, baseOutputFile string) (mutationEventPath, mutationPath string) {
	// Use absolute path if provided, otherwise join with parquet directory
	var basePath string
	if filepath.IsAbs(baseOutputFile) {
		basePath = baseOutputFile
	} else {
		basePath = filepath.Join(parquetDir, baseOutputFile)
	}

	// Remove .parquet extension if present
	basePathWithoutExt := strings.TrimSuffix(basePath, ".parquet")

	// Create two output paths
	mutationEventPath = basePathWithoutExt + "_mutation_event.parquet"
	mutationPath = basePathWithoutExt + "_mutation.parquet"

	return mutationEventPath, mutationPath
}
