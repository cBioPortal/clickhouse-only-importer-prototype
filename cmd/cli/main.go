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
)

// example workflows:

// convert mode:
//go run ./cmd/cli/main.go -mode convert -tsv-dir ./data -parquet-dir ./output

// combine mode
//go run ./cmd/cli/main.go -mode combine -parquet-dir ./output -output final.parquet

func main() {
	// command-line flags
	mode := flag.String(
		"mode",
		"convert",
		"Operation mode: 'convert' (TSV to parquet) or 'combine' (merge parquet files)",
	)
	tsvRootDir := flag.String("tsv-dir", "", "Root directory containing TSV files (convert mode)")
	parquetDir := flag.String(
		"parquet-dir",
		"",
		"Directory for parquet files (input for combine mode, output for convert mode)",
	)
	outputFile := flag.String(
		"output",
		"combined-all-cna.parquet",
		"Output filename for combined parquet (combine mode)",
	)
	flag.Parse()

	// validate mode
	if *mode != "convert" && *mode != "combine" {
		log.Fatal("Invalid mode. Must be 'convert' or 'combine'")
	}

	// validate required flags
	if *parquetDir == "" {
		log.Fatal("Error: -parquet-dir is required")
	}

	mem := memory.DefaultAllocator
	startTime := time.Now()

	switch *mode {
	case "convert":
		if *tsvRootDir == "" {
			log.Fatal("Error: -tsv-dir is required for convert mode")
		}
		runConvertMode(*tsvRootDir, *parquetDir, mem)

	case "combine":
		runCombineMode(*parquetDir, *outputFile, mem)
	}

	elapsed := time.Since(startTime)
	log.Printf("Total execution time: %s", elapsed)
}

func runConvertMode(tsvRootDir, parquetDir string, mem memory.Allocator) {
	log.Print("=== CONVERT MODE: TSV to individual Parquet files ===")
	log.Printf("Scanning for data_CNA.txt files in: %s", tsvRootDir)

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

	log.Print("Transforming TSV files and writing individual parquet files (streaming)")
	if err := cna.ProcessMultipleTSVToParquet(cnaFiles, parquetDir, mem); err != nil {
		log.Fatalf("Error processing TSV files: %v", err)
	}

	log.Printf("✓ Individual parquet files written to: %s", parquetDir)
	log.Printf("✓ Number of CNA files processed: %d", len(cnaFiles))
}

func runCombineMode(parquetDir, outputFile string, mem memory.Allocator) {
	log.Print("=== COMBINE MODE: Merge Parquet files into single file ===")
	log.Printf("Reading parquet files from: %s", parquetDir)

	// Use absolute path if provided, otherwise join with parquet directory
	var combinedParquetPath string
	if filepath.IsAbs(outputFile) {
		combinedParquetPath = outputFile
	} else {
		combinedParquetPath = filepath.Join(parquetDir, outputFile)
	}
	log.Printf("Combining all parquet files into: %s", combinedParquetPath)

	if err := cna.CombineParquetFiles(parquetDir, combinedParquetPath, mem); err != nil {
		log.Fatalf("Error combining parquet files: %v", err)
	}

	log.Printf("✓ Combined parquet file written to: %s", combinedParquetPath)
}

func findCNAFiles(rootDir string) ([]cna.CNAFileInput, error) {
	var cnaFiles []cna.CNAFileInput
	// Maps to track files by directory
	metaFilesByDir := make(map[string]string)
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
			metaFilesByDir[dir] = path
			return nil
		}

		// Check for data file
		dataMatched, _ := regexp.MatchString(`^data_.*cna.*\.txt$`, fileName)
		if dataMatched {
			dataFilesByDir[dir] = append(dataFilesByDir[dir], path)
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	// second pass: process data files with their paired meta files
	for dir, dataPaths := range dataFilesByDir {
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

			// Look for paired meta file in same directory
			var cancerStudyId, stableId string
			if metaPath, hasMetaFile := metaFilesByDir[dir]; hasMetaFile {
				cancerStudyId, stableId, err = extractMetadata(metaPath)
				if err != nil {
					log.Printf("warning: failed to extract metadata from %s: %v", metaPath, err)
				}
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

// extractMetadata reads a meta file and extracts cancer_study_identifier and stable_id
func extractMetadata(metaPath string) (cancerStudyId, stableId string, err error) {
	file, err := os.Open(metaPath)
	if err != nil {
		return "", "", err
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
		}

		// Early exit if we found both
		if cancerStudyId != "" && stableId != "" {
			break
		}
	}

	if err := scanner.Err(); err != nil {
		return "", "", err
	}

	return cancerStudyId, stableId, nil
}
