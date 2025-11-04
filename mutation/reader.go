package mutation

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/csv"
)

// GetFieldTypesFromFile infers an Arrow schema from the header row of a TSV file.
func GetFieldTypesFromFile(f *os.File, delimiter rune) (*arrow.Schema, error) {
	rdr := bufio.NewReader(f)

	// Read lines until we find a non-comment line (the header)
	var line string
	var err error
	for {
		line, err = rdr.ReadString('\n')
		if err != nil {
			return nil, fmt.Errorf("failed to read header line: %w", err)
		}
		// Skip lines starting with '#' (but don't trim tabs - they might be significant)
		trimmed := strings.TrimSpace(line)
		if trimmed != "" && !strings.HasPrefix(trimmed, "#") {
			break
		}
	}

	// Remove only newline/carriage return, not tabs (tabs are field delimiters)
	line = strings.TrimRight(line, "\n\r")
	header := strings.Split(line, string(delimiter))
	fields := make([]arrow.Field, len(header))
	for i, h := range header {
		fields[i] = arrow.Field{Name: h, Type: arrow.BinaryTypes.String}
	}

	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return nil, fmt.Errorf("failed to rewind file: %w", err)
	}

	return arrow.NewSchema(fields, nil), nil
}

// ReadTSVAsRecords streams arrow record batches from a tsv file.
func ReadTSVAsRecords(
	tsvPath string,
	schema *arrow.Schema,
	chunkSize int,
	delimiter rune,
) (*csv.Reader, *os.File, error) {
	f, err := os.Open(tsvPath)
	if err != nil {
		return nil, nil, fmt.Errorf("cannot open TSV file %q: %w", tsvPath, err)
	}

	rdr := csv.NewReader(
		f,
		schema,
		csv.WithHeader(true),
		csv.WithChunk(chunkSize),
		csv.WithComma(delimiter),
		csv.WithComment('#'),
	)
	return rdr, f, nil
}
