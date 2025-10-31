# clickhouse-only-importer-prototype

## Modes

### convert mode
Converts TSV files to two parquet files per study:
- `*_genetic_alterations.parquet` - patterned after cgds.sql genetic_alteration
- `*_genetic_profile_samples.parquet` - patterned after cgds.sql genetic_profile_samples

```bash
go run ./cmd/cli/main.go -mode convert -tsv-dir ./data -parquet-dir ./output
```

where:
- `-tsv-dir`: Path to the root directory containing study data (/datahub/public)
- `-parquet-dir`: Path to place individual CNA parquet files

### convert-with-derived mode
Converts TSV files to three parquet files per study (includes derived denormalized format):
- `*_genetic_alterations.parquet` - patterned after cgds.sql genetic_alteration
- `*_genetic_profile_samples.parquet` - patterned after cgds.sql genetic_profile_samples
- `*_derived.parquet` - derived record - gene-sample-measurement

```bash
go run ./cmd/cli/main.go -mode convert-with-derived -tsv-dir ./data -parquet-dir ./output
```

The derived format contains one row per sample-gene combination with columns:
- `SAMPLE_ID` - Sample identifier
- `CANCER_STUDY` - Cancer study identifier
- `GENE_SYMBOL` - Gene symbol
- `GENETIC_PROFILE` - Genetic profile identifier
- `ALTERATION` - CNA alteration value

### combine mode
Combines individual parquet files into two final parquet files:
- `combined-*_genetic_alterations.parquet`
- `combined-*_genetic_profile_samples.parquet`

```bash
go run ./cmd/cli/main.go -mode combine -parquet-dir ./output -output combined-all-cna.parquet
```

where:
- `-parquet-dir`: Path where individual CNA parquet files reside
- `-output`: Base filename for combined parquet files (will generate two files with different suffixes)

### combine-with-derived mode
Combines individual parquet files into three final parquet files (includes derived):
- `combined-*_genetic_alterations.parquet`
- `combined-*_genetic_profile_samples.parquet`
- `combined-*_derived.parquet`

```bash
go run ./cmd/cli/main.go -mode combine-with-derived -parquet-dir ./output -output combined-all-cna.parquet
```

where:
- `-parquet-dir`: Path where individual CNA parquet files reside
- `-output`: Base filename for combined parquet files (will generate three files with different suffixes)
