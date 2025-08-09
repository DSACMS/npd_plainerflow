#!/usr/bin/env python3
"""
Test script for CredentialFinder functionality.

This script demonstrates the basic usage of CredentialFinder and tests
the SQLite fallback functionality.
"""

import sys
import os
from pathlib import Path

# Add the parent directory to the path so we can import npd_plainerflow
sys.path.insert(0, str(Path(__file__).parent.parent))

import npd_plainerflow
from npd_plainerflow import CredentialFinder


def test_sqlite_fallback():
    """Test the SQLite fallback functionality."""
    print("Testing SQLite fallback...")
    
    # Test with default fallback
    engine = CredentialFinder.detect_config(verbose=True)
    print(f"Engine created: {engine}")
    print(f"Engine URL: {engine.url}")
    
    # Test a simple query
    with engine.connect() as conn:
        from sqlalchemy import text
        result = conn.execute(text("SELECT 1 as test_value"))
        row = result.fetchone()
        print(f"Test query result: {row}")
    
    print("✓ SQLite fallback test passed!\n")


def test_sqlite_override():
    """Test the SQLite override functionality."""
    print("Testing SQLite override...")
    
    # Test with custom SQLite file
    custom_db = "/tmp/test_npd_plainerflow.db"
    engine = CredentialFinder.detect_config(
        sqlite_db_file=custom_db, 
        verbose=True
    )
    print(f"Engine created: {engine}")
    print(f"Engine URL: {engine.url}")
    
    # Test a simple query
    with engine.connect() as conn:
        from sqlalchemy import text
        result = conn.execute(text("SELECT 'custom db' as test_value"))
        row = result.fetchone()
        print(f"Test query result: {row}")
    
    # Clean up
    if os.path.exists(custom_db):
        os.remove(custom_db)
    
    print("✓ SQLite override test passed!\n")


def test_env_file_missing():
    """Test behavior when .env file is missing."""
    print("Testing missing .env file...")
    
    # Test with non-existent .env file
    engine = CredentialFinder.detect_config(
        env_path="/nonexistent/.env",
        verbose=True
    )
    print(f"Engine created (should fallback to SQLite): {engine}")
    print(f"Engine URL: {engine.url}")
    
    print("✓ Missing .env file test passed!\n")


def main():
    """Run all tests."""
    print("=== CredentialFinder Test Suite ===\n")
    
    try:
        test_sqlite_fallback()
        test_sqlite_override()
        test_env_file_missing()
        
        print("🎉 All tests passed!")
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
