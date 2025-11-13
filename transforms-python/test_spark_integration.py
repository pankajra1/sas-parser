"""
Test Spark integration for SAS parser.
This test verifies the parser works with Spark DataFrames.
"""
import os
import sys

# Add the source directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src', 'main', 'python'))

from pyspark.sql import SparkSession
from com.parser.sas.sas_dataset_parser_factory import SASDatasetParserFactory


def test_spark_integration():
    """Test that the parser works with Spark"""
    print("Testing Spark Integration...")
    print("=" * 60)
    
    try:
        # Create Spark session
        spark = SparkSession.builder \
            .appName("SASParserTest") \
            .master("local[1]") \
            .getOrCreate()
        
        print("[PASS] Spark session created")
        
        # Create parser
        parser = SASDatasetParserFactory.create()
        print("[PASS] Parser created")
        
        # Create a test DataFrame with file information
        # Using empty content for now (would need actual SAS files for full test)
        test_data = [
            ("test1.sas7bdat", b"dummy"),
            ("test2.xpt", b"dummy"),
            ("test3.txt", b"dummy")  # Unsupported format
        ]
        
        files_df = spark.createDataFrame(test_data, ["logical_path", "content"])
        print("[PASS] Test DataFrame created")
        
        # Parse files
        try:
            parsed_df = parser.parse(files_df)
            print("[PASS] Parser executed successfully")
            
            # Check schema
            schema_fields = parsed_df.schema.fieldNames()
            expected_fields = [
                'logical_path', 'dataset_label', 'row_index', 'column_index',
                'column_name', 'column_label', 'column_format', 'data_type',
                'value_string', 'value_double', 'value_date', 'value_time',
                'value_timestamp', 'error_msg'
            ]
            
            assert len(schema_fields) == 14, f"Expected 14 fields, got {len(schema_fields)}"
            for field in expected_fields:
                assert field in schema_fields, f"Missing field: {field}"
            
            print("[PASS] Schema validation passed")
            
            # Count rows
            row_count = parsed_df.count()
            print(f"[INFO] Generated {row_count} rows from test files")
            
            # Show sample (will show error rows for dummy files)
            print("\nSample output (first 5 rows):")
            parsed_df.show(5, truncate=False)
            
        except Exception as e:
            print(f"[FAIL] Parsing failed: {e}")
            import traceback
            traceback.print_exc()
            return False
        
        spark.stop()
        print("[PASS] Spark session stopped")
        
        print("\n" + "=" * 60)
        print("Spark integration test passed! [PASS]")
        return True
        
    except Exception as e:
        print(f"[FAIL] Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    print("SAS Parser - Spark Integration Test")
    print("=" * 60)
    
    if not test_spark_integration():
        sys.exit(1)
    
    print("\n" + "=" * 60)
    print("All Spark integration tests passed! [PASS]")
    print("=" * 60)

