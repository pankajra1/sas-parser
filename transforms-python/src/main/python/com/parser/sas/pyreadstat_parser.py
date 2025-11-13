"""
PyReadstat-based SAS Parser Implementation
Replaces JDBC-based parsing with pyreadstat library
"""
import os
import tempfile
import logging
from typing import Iterator
from datetime import datetime, date, time
import pyreadstat
import pandas as pd
from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    DoubleType, DateType, TimestampType
)

from .sas_dataset_parser import SASDatasetParser

logger = logging.getLogger(__name__)


class PyReadstatParser(SASDatasetParser):
    """
    PyReadstat-based implementation of SASDatasetParser.
    Uses pyreadstat library instead of JDBC to read SAS files.
    """
    
    # Define the schema matching Java implementation exactly
    SCHEMA = StructType([
        StructField("logical_path", StringType(), True),
        StructField("dataset_label", StringType(), True),
        StructField("row_index", IntegerType(), True),
        StructField("column_index", IntegerType(), True),
        StructField("column_name", StringType(), True),
        StructField("column_label", StringType(), True),
        StructField("column_format", StringType(), True),
        StructField("data_type", StringType(), True),
        StructField("value_string", StringType(), True),
        StructField("value_double", DoubleType(), True),
        StructField("value_date", DateType(), True),
        StructField("value_time", IntegerType(), True),
        StructField("value_timestamp", TimestampType(), True),
        StructField("error_msg", StringType(), True)
    ])
    
    def parse(self, files_df: DataFrame) -> DataFrame:
        """
        Parse SAS files using Spark flatMap for parallel processing.
        Matches the Java implementation using flatMap.
        
        Args:
            files_df: DataFrame with columns: logical_path (string), content (binary)
            
        Returns:
            DataFrame with parsed SAS data in skinny format
        """
        spark = SparkSession.getActiveSession()
        if spark is None:
            raise RuntimeError("No active Spark session found")
        
        # Convert to RDD for flatMap operation (matches Java flatMap approach)
        # This allows parallel processing of multiple files
        def parse_file_partition(iterator):
            """Process a partition of files"""
            for row in iterator:
                # Access row fields - handle both Row objects and tuples
                if hasattr(row, 'logical_path'):
                    logical_path = row.logical_path
                    content = row.content if hasattr(row, 'content') else row[1]
                else:
                    # Assume tuple/list format: (logical_path, content)
                    logical_path = row[0]
                    content = row[1]
                
                # Yield all rows from parsing this file
                for parsed_row in _parse_single_file(logical_path, content):
                    yield parsed_row
        
        # Use flatMap on RDD for parallel processing (matches Java implementation)
        parsed_rdd = files_df.rdd.flatMap(parse_file_partition)
        
        # Convert back to DataFrame with schema
        parsed_df = spark.createDataFrame(parsed_rdd, self.SCHEMA)
        
        return parsed_df


def _parse_single_file(logical_path: str, content: bytes) -> Iterator[Row]:
    """
    Parse a single SAS file and yield rows in skinny format.
    This matches the logic of SJDBCRowIterator in Java.
    
    Args:
        logical_path: Logical path of the file
        content: Binary content of the file
        
    Yields:
        Row objects matching the schema
    """
    error_msg = None
    temp_file = None
    temp_dir = None
    
    try:
        # Check file extension
        if not (logical_path.lower().endswith('.sas7bdat') or logical_path.lower().endswith('.xpt')):
            error_msg = f"Unsupported SAS dataset (only .sas7bdat and .xpt supported): {logical_path}"
            logger.error(error_msg)
            yield _create_error_row(logical_path, error_msg)
            return
        
        # Create temporary file
        temp_dir = tempfile.mkdtemp(prefix='sas_parser_')
        file_ext = '.xpt' if logical_path.lower().endswith('.xpt') else '.sas7bdat'
        temp_file = os.path.join(temp_dir, f'tmp{file_ext}')
        
        with open(temp_file, 'wb') as f:
            f.write(content)
        
        # Read SAS file using pyreadstat
        if logical_path.lower().endswith('.xpt'):
            # For XPT files, read_xport returns a tuple (df, meta)
            # Note: pyreadstat.read_xport reads the first dataset by default
            # For multiple datasets, we'd need to use read_xport with metadata_only=True and iterate
            # But matching Java logic: only single table supported
            try:
                df, meta = pyreadstat.read_xport(temp_file)
            except Exception as e:
                # Check if it's a multiple dataset issue
                error_msg = f"Failed to read XPT file: {str(e)}"
                logger.error(f"{error_msg}: {logical_path}", exc_info=True)
                yield _create_error_row(logical_path, error_msg)
                return
        else:
            df, meta = pyreadstat.read_sas7bdat(temp_file)
        
        # Check if empty
        if df is None or len(df) == 0:
            error_msg = "Empty SAS dataset"
            logger.warning(f"{error_msg}: {logical_path}")
            yield _create_error_row(logical_path, error_msg)
            return
        
        # Extract metadata
        dataset_label = getattr(meta, 'table_name', "") or getattr(meta, 'file_label', "") or ""
        col_names = list(df.columns)
        col_count = len(col_names)
        
        # Get column metadata
        col_labels = []
        col_formats = []
        col_types = []
        
        # Extract variable labels and formats from metadata
        variable_labels = getattr(meta, 'variable_labels', {}) or {}
        variable_formats = getattr(meta, 'variable_formats', {}) or {}
        variable_types = getattr(meta, 'variable_types', {}) or {}
        
        for col_name in col_names:
            col_labels.append(variable_labels.get(col_name, ""))
            col_format = variable_formats.get(col_name, "")
            col_formats.append(col_format)
            # Determine type from pandas dtype and format
            col_types.append(_determine_data_type(df[col_name].dtype, col_format))
        
        # Process each row and column (pivot to skinny format)
        for row_index in range(len(df)):
            for col_index in range(col_count):
                row_data = df.iloc[row_index]
                col_name = col_names[col_index]
                col_label = col_labels[col_index]
                col_format = col_formats[col_index]
                col_type = col_types[col_index]
                value = row_data[col_name]
                
                # Create row in skinny format
                yield _create_data_row(
                    logical_path=logical_path,
                    dataset_label=dataset_label,
                    row_index=row_index,
                    column_index=col_index,
                    column_name=col_name,
                    column_label=col_label,
                    column_format=col_format,
                    data_type=col_type,
                    value=value,
                    pandas_dtype=df[col_name].dtype
                )
    
    except Exception as e:
        error_msg = f"Failed to parse SAS file: {str(e)}"
        logger.error(f"Failed to parse {logical_path}", exc_info=True)
        yield _create_error_row(logical_path, error_msg)
    
    finally:
        # Cleanup temporary files
        if temp_file and os.path.exists(temp_file):
            try:
                os.remove(temp_file)
            except Exception as e:
                logger.error(f"Failed to delete temp file {temp_file}", exc_info=True)
        
        if temp_dir and os.path.exists(temp_dir):
            try:
                os.rmdir(temp_dir)
            except Exception as e:
                logger.error(f"Failed to delete temp dir {temp_dir}", exc_info=True)


def _determine_data_type(pandas_dtype, column_format: str) -> str:
    """
    Map pandas dtype and SAS format to Java SQL Types equivalent.
    Returns type name: "String", "Double", "Date", "Time", "Timestamp"
    Matches the Java logic for type determination.
    """
    # Check format first for date/time types
    if column_format:
        format_upper = column_format.upper()
        if format_upper.startswith("DATE") or format_upper.startswith("YYMMDD"):
            return "Date"
        elif format_upper.startswith("TIME") or format_upper.startswith("HHMM"):
            return "Time"
        elif format_upper.startswith("DATETIME") or format_upper.startswith("DT"):
            return "Timestamp"
        elif format_upper.startswith("TOD"):
            return "Timestamp"  # Will be converted to Time in _create_data_row
    
    # Fall back to pandas dtype
    if pd.api.types.is_string_dtype(pandas_dtype) or pd.api.types.is_object_dtype(pandas_dtype):
        return "String"
    elif pd.api.types.is_integer_dtype(pandas_dtype) or pd.api.types.is_float_dtype(pandas_dtype):
        return "Double"
    elif pd.api.types.is_datetime64_any_dtype(pandas_dtype):
        return "Timestamp"
    else:
        return "String"  # Default to String


def _create_data_row(
    logical_path: str,
    dataset_label: str,
    row_index: int,
    column_index: int,
    column_name: str,
    column_label: str,
    column_format: str,
    data_type: str,
    value: any,
    pandas_dtype: any
) -> Row:
    """
    Create a data row in skinny format.
    Matches the logic of addValues() method in Java SJDBCRowIterator.
    """
    # Initialize all fields
    value_string = None
    value_double = None
    value_date = None
    value_time = None
    value_timestamp = None
    error_msg = None
    
    try:
        # Handle missing values
        if pd.isna(value):
            # All value fields remain None
            pass
        elif data_type == "String":
            value_string = str(value) if value is not None else None
        elif data_type == "Double":
            value_double = float(value) if value is not None else None
        elif data_type == "Date":
            # SAS dates are typically numeric (days since 1960-01-01) or datetime
            if isinstance(value, date):
                value_date = value
            elif isinstance(value, datetime):
                value_date = value.date()
            elif pd.api.types.is_datetime64_any_dtype(type(value)):
                value_date = pd.Timestamp(value).date()
            elif isinstance(value, (int, float)) and not pd.isna(value):
                # SAS date: days since 1960-01-01
                try:
                    value_date = date(1960, 1, 1) + pd.Timedelta(days=int(value))
                except (ValueError, OverflowError):
                    pass  # Invalid date, leave as None
        elif data_type == "Time":
            # SAS time: seconds since midnight
            if isinstance(value, time):
                value_string = value.strftime("%H:%M:%S")
                value_time = value.hour * 3600 + value.minute * 60 + value.second
            elif isinstance(value, datetime):
                local_time = value.time()
                value_string = local_time.strftime("%H:%M:%S")
                value_time = local_time.hour * 3600 + local_time.minute * 60 + local_time.second
            elif pd.api.types.is_datetime64_any_dtype(type(value)):
                ts = pd.Timestamp(value)
                local_time = ts.time()
                value_string = local_time.strftime("%H:%M:%S")
                value_time = local_time.hour * 3600 + local_time.minute * 60 + local_time.second
            elif isinstance(value, (int, float)) and not pd.isna(value):
                # SAS time: seconds since midnight
                try:
                    seconds = int(value)
                    hours = seconds // 3600
                    minutes = (seconds % 3600) // 60
                    secs = seconds % 60
                    value_time = seconds
                    value_string = f"{hours:02d}:{minutes:02d}:{secs:02d}"
                except (ValueError, OverflowError):
                    pass  # Invalid time, leave as None
        elif data_type == "Timestamp":
            # Check for TOD format (Time of Day) - special handling matching Java logic
            if column_format and column_format.upper().startswith("TOD"):
                # Convert timestamp to time (matching Java LocalDateTime.ofEpochSecond logic)
                if isinstance(value, datetime):
                    # Use UTC epoch seconds (matching Java logic)
                    epoch_seconds = int(value.timestamp())
                    # Convert to UTC LocalDateTime then extract time
                    dt_utc = datetime.utcfromtimestamp(epoch_seconds)
                    local_time = dt_utc.time()
                    value_string = local_time.strftime("%H:%M:%S")
                    value_time = local_time.hour * 3600 + local_time.minute * 60 + local_time.second
                    data_type = "Time"
                elif pd.api.types.is_datetime64_any_dtype(type(value)):
                    ts = pd.Timestamp(value)
                    # Convert to UTC
                    ts_utc = ts.tz_localize(None) if ts.tz is None else ts.tz_convert('UTC').tz_localize(None)
                    local_time = ts_utc.time()
                    value_string = local_time.strftime("%H:%M:%S")
                    value_time = local_time.hour * 3600 + local_time.minute * 60 + local_time.second
                    data_type = "Time"
                elif isinstance(value, (int, float)) and not pd.isna(value):
                    # SAS datetime: seconds since 1960-01-01, convert to time
                    try:
                        base_dt = datetime(1960, 1, 1)
                        dt = base_dt + pd.Timedelta(seconds=float(value))
                        local_time = dt.time()
                        value_string = local_time.strftime("%H:%M:%S")
                        value_time = local_time.hour * 3600 + local_time.minute * 60 + local_time.second
                        data_type = "Time"
                    except (ValueError, OverflowError):
                        pass
            else:
                # Regular timestamp
                if isinstance(value, datetime):
                    value_timestamp = value
                elif pd.api.types.is_datetime64_any_dtype(type(value)):
                    value_timestamp = pd.Timestamp(value).to_pydatetime()
                elif isinstance(value, (int, float)) and not pd.isna(value):
                    # SAS datetime: seconds since 1960-01-01
                    try:
                        value_timestamp = datetime(1960, 1, 1) + pd.Timedelta(seconds=float(value))
                    except (ValueError, OverflowError):
                        pass  # Invalid timestamp, leave as None
        else:
            error_msg = f"Unsupported data type {data_type}"
            logger.error(f"{error_msg} found in {logical_path} row {row_index} col {column_index}")
    
    except Exception as e:
        error_msg = f"Failed to get value: {str(e)}"
        logger.error(f"Failed to get value for {logical_path} row {row_index} col {column_index}", exc_info=True)
    
    return Row(
        logical_path=logical_path,
        dataset_label=dataset_label,
        row_index=row_index,
        column_index=column_index,
        column_name=column_name,
        column_label=column_label,
        column_format=column_format,
        data_type=data_type,
        value_string=value_string,
        value_double=value_double,
        value_date=value_date,
        value_time=value_time,
        value_timestamp=value_timestamp,
        error_msg=error_msg
    )


def _create_error_row(logical_path: str, error_msg: str) -> Row:
    """Create an error row with only logical_path and error_msg filled"""
    return Row(
        logical_path=logical_path,
        dataset_label=None,
        row_index=None,
        column_index=None,
        column_name=None,
        column_label=None,
        column_format=None,
        data_type=None,
        value_string=None,
        value_double=None,
        value_date=None,
        value_time=None,
        value_timestamp=None,
        error_msg=error_msg
    )

