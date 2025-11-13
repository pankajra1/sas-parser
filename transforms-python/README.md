# SAS Parser - Python Implementation

## Overview

This is a Python implementation of the SAS file parser that replaces the Java JDBC-based implementation with a `pyreadstat`-based solution. The implementation maintains exact compatibility with the Java version, including:

- **Exact schema matching**: 14 fields matching the Java implementation
- **Skinny format output**: Pivots wide SAS data to thin/skinny format (one row per cell)
- **Spark parallel processing**: Uses Spark's `flatMap` for distributed processing of multiple files
- **Large file support**: Efficiently handles large SAS files through streaming and temporary file management
- **Data type handling**: Supports String, Double, Date, Time, and Timestamp types with special handling for TOD (Time of Day) formats
- **Error handling**: Comprehensive error handling matching Java implementation behavior

## Architecture

The implementation follows the same structure as the Java code:

1. **SASDatasetParser** (Interface): Defines the parser contract
2. **PyReadstatParser** (Implementation): Uses `pyreadstat` library to read SAS files
3. **SASDatasetParserFactory**: Factory for creating parser instances

## Key Features

### File Format Support
- `.sas7bdat` files (SAS binary datasets)
- `.xpt` files (SAS Transport files) - supports single table per file

### Data Type Mapping
- **String**: Character/string data → `value_string`
- **Double**: Numeric data → `value_double`
- **Date**: Date values (days since 1960-01-01) → `value_date`
- **Time**: Time values (seconds since midnight) → `value_string` and `value_time`
- **Timestamp**: DateTime values → `value_timestamp`
- **TOD Format**: Special handling for Time of Day timestamps (converted to Time type)

### Output Schema

The parser outputs data in a skinny format with the following 14 fields:

1. `logical_path` (String): Path to the source file
2. `dataset_label` (String): Label/name of the dataset
3. `row_index` (Integer): Row number (0-based)
4. `column_index` (Integer): Column number (0-based)
5. `column_name` (String): Name of the column
6. `column_label` (String): Label of the column
7. `column_format` (String): SAS format string
8. `data_type` (String): Data type ("String", "Double", "Date", "Time", "Timestamp")
9. `value_string` (String): String representation of the value
10. `value_double` (Double): Numeric value
11. `value_date` (Date): Date value
12. `value_time` (Integer): Time value in seconds since midnight
13. `value_timestamp` (Timestamp): Timestamp value
14. `error_msg` (String): Error message if parsing failed

## Usage

### Basic Usage

```python
from pyspark.sql import SparkSession
from com.parser.sas.sas_dataset_parser_factory import SASDatasetParserFactory

# Create Spark session
spark = SparkSession.builder.appName("SASParser").getOrCreate()

# Create parser
parser = SASDatasetParserFactory.create()

# Prepare files DataFrame
# files_df should have columns: logical_path (string), content (binary)
files_df = spark.createDataFrame([
    ("file1.sas7bdat", binary_content1),
    ("file2.xpt", binary_content2)
], ["logical_path", "content"])

# Parse files
parsed_df = parser.parse(files_df)

# Process results
parsed_df.show()
```

### With PortableFile-like Structure

If you have a DataFrame with file information (similar to Java's PortableFile):

```python
# Assuming files_df has logical_path and content columns
parsed_df = parser.parse(files_df)

# Filter for specific files or error rows
error_rows = parsed_df.filter(parsed_df.error_msg.isNotNull())
data_rows = parsed_df.filter(parsed_df.error_msg.isNull())
```

## Installation

```bash
pip install -r requirements.txt
```

## Dependencies

- **pyreadstat**: Library for reading SAS files (replaces JDBC driver)
- **pandas**: Data manipulation and type handling
- **pyspark**: Spark integration for parallel processing
- **numpy**: Numerical operations (optional but recommended)

## Differences from Java Implementation

1. **Library**: Uses `pyreadstat` instead of Carolina JDBC driver
2. **No License File**: `pyreadstat` is open-source and doesn't require a license file
3. **Python Types**: Uses Python/Pandas types internally but maintains same output schema

## Performance Considerations

- **Parallel Processing**: Files are processed in parallel using Spark's distributed processing
- **Temporary Files**: Each file is written to a temporary location for `pyreadstat` to read
- **Memory Management**: Large files are handled efficiently through streaming and cleanup
- **Error Isolation**: Errors in one file don't affect processing of other files

## Error Handling

The parser handles various error conditions:

- Unsupported file formats (only .sas7bdat and .xpt)
- Empty SAS datasets
- Multiple tables in XPT files (only single table supported)
- Parsing errors (connection failures, read errors)
- Invalid data types or values

All errors are captured in the `error_msg` field of the output schema.

## Testing

The implementation is designed to match the Java test cases. Key test scenarios:

- Column count validation
- Column names validation
- Column types validation
- Column formats validation
- Row count validation
- Special characters in column names
- Column length validation

## Notes

- The implementation maintains exact compatibility with the Java version
- All data type conversions match the Java JDBC type mappings
- TOD format timestamps are handled specially (converted to Time type)
- SAS date/time values use the standard SAS epoch (1960-01-01)

