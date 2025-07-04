#!/usr/bin/env python3
"""
Script to install plainerflow package and test basic functionality
"""
import sys
import subprocess
import os


def run_command(cmd, description):
    """Run a command and handle errors"""
    print(f"\n{description}...")
    print(f"Running: {cmd}")
    try:
        result = subprocess.run(cmd, shell=True, check=True, capture_output=True, text=True)
        print(f"✓ {description} completed successfully")
        if result.stdout:
            print(f"Output: {result.stdout}")
        return True
    except subprocess.CalledProcessError as e:
        print(f"✗ {description} failed")
        print(f"Error: {e.stderr}")
        return False


def check_import():
    """Test importing plainerflow"""
    print("\nTesting plainerflow import...")
    try:
        import plainerflow
        print(f"✓ Successfully imported plainerflow version {plainerflow.__version__}")
        return True
    except ImportError as e:
        print(f"✗ Failed to import plainerflow: {e}")
        return False


def check_sqlalchemy():
    """Test SQLAlchemy dependency"""
    print("\nTesting SQLAlchemy dependency...")
    try:
        import sqlalchemy
        print(f"✓ SQLAlchemy is available (version: {sqlalchemy.__version__})")
        return True
    except ImportError as e:
        print(f"✗ SQLAlchemy not available: {e}")
        return False


def main():
    """Main function"""
    print("=== plainerflow Installation and Test Script ===")
    
    # Check if we're in the right directory
    if not os.path.exists("plainerflow/__init__.py"):
        print("✗ Error: This script should be run from the plainerflow project root directory")
        sys.exit(1)
    
    # Install in development mode
    if not run_command("pip install -e .", "Installing plainerflow in development mode"):
        sys.exit(1)
    
    # Test imports
    if not check_import():
        sys.exit(1)
    
    if not check_sqlalchemy():
        sys.exit(1)
    
    # Run tests if pytest is available
    if run_command("python -m pytest tests/ -v", "Running tests"):
        print("\n✓ All tests passed!")
    else:
        print("\n! Tests failed or pytest not available")
    
    print("\n=== Installation and testing completed ===")
    print("You can now use: import plainerflow")


if __name__ == "__main__":
    main()
