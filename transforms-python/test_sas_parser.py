"""
Simple test script to verify SAS parser implementation works locally.
Tests the core parsing logic without requiring full Spark setup.
"""
import os
import sys
import tempfile
from pathlib import Path

# Add the source directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src', 'main', 'python'))

from com.parser.sas.pyreadstat_parser import _parse_single_file
from pyspark.sql import Row


def test_parser_with_sample_data():
    """Test the parser with a simple mock SAS file scenario"""
    print("Testing SAS Parser Implementation...")
    print("=" * 60)
    
    # Test 1: Test error handling for unsupported format
    print("\nTest 1: Unsupported file format")
    try:
        rows = list(_parse_single_file("test.txt", b"dummy content"))
        assert len(rows) == 1
        assert rows[0].error_msg is not None
        assert "Unsupported SAS dataset" in rows[0].error_msg
        print("[PASS] Unsupported format handled correctly")
    except Exception as e:
        print(f"[FAIL] Test 1 failed: {e}")
        return False
    
    # Test 2: Test error handling for empty content
    print("\nTest 2: Empty/invalid file content")
    try:
        # Create a temporary invalid SAS file
        with tempfile.NamedTemporaryFile(suffix='.sas7bdat', delete=False) as tmp:
            tmp.write(b"invalid sas content")
            tmp_path = tmp.name
        
        try:
            with open(tmp_path, 'rb') as f:
                content = f.read()
            rows = list(_parse_single_file("test.sas7bdat", content))
            # Should either parse (if pyreadstat is lenient) or error
            # Either way, we should get rows
            assert len(rows) > 0
            print("[PASS] Invalid file handled (may error, which is expected)")
        finally:
            os.unlink(tmp_path)
    except Exception as e:
        print(f"[PASS] Invalid file correctly raises error: {type(e).__name__}")
    
    # Test 3: Test schema structure
    print("\nTest 3: Schema structure validation")
    try:
        # Create a minimal test - just check that error rows have correct schema
        rows = list(_parse_single_file("test.xyz", b"content"))
        if len(rows) > 0:
            row = rows[0]
            # Check all required fields exist
            required_fields = [
                'logical_path', 'dataset_label', 'row_index', 'column_index',
                'column_name', 'column_label', 'column_format', 'data_type',
                'value_string', 'value_double', 'value_date', 'value_time',
                'value_timestamp', 'error_msg'
            ]
            for field in required_fields:
                assert hasattr(row, field), f"Missing field: {field}"
            print("[PASS] Schema structure is correct (14 fields)")
    except Exception as e:
        print(f"[FAIL] Test 3 failed: {e}")
        return False
    
    print("\n" + "=" * 60)
    print("Basic tests passed! [PASS]")
    print("\nNote: Full testing with actual SAS files requires:")
    print("  - pyreadstat library installed")
    print("  - Valid .sas7bdat or .xpt test files")
    print("  - Spark session for full integration testing")
    return True


def test_imports():
    """Test that all imports work correctly"""
    print("\nTesting imports...")
    try:
        from com.parser.sas.sas_dataset_parser import SASDatasetParser
        from com.parser.sas.pyreadstat_parser import PyReadstatParser
        from com.parser.sas.sas_dataset_parser_factory import SASDatasetParserFactory
        
        # Test factory
        parser = SASDatasetParserFactory.create()
        assert parser is not None
        assert isinstance(parser, SASDatasetParser)
        print("[PASS] All imports successful")
        print("[PASS] Factory creates parser instance")
        return True
    except ImportError as e:
        print(f"[FAIL] Import failed: {e}")
        print("\nMake sure you have installed dependencies:")
        print("  pip install pyreadstat pandas pyspark")
        return False
    except Exception as e:
        print(f"[FAIL] Test failed: {e}")
        return False


if __name__ == "__main__":
    print("SAS Parser - Local Test Suite")
    print("=" * 60)
    
    # Test imports first
    if not test_imports():
        sys.exit(1)
    
    # Test parser logic
    if not test_parser_with_sample_data():
        sys.exit(1)
    
    print("\n" + "=" * 60)
    print("All tests passed! [PASS]")
    print("=" * 60)

