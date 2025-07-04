"""
Demonstration of InLaw dynamic import functionality.
"""

import sqlalchemy
from plainerflow import InLaw


def main():
    """Demonstrate InLaw dynamic import functionality."""
    
    # Create a simple in-memory SQLite database for demonstration
    engine = sqlalchemy.create_engine("sqlite:///:memory:")
    
    print("=== InLaw Dynamic Import Demonstration ===\n")
    
    # Test 1: Run tests from current namespace only
    print("1. Running tests from current namespace only:")
    results1 = InLaw.run_all(engine=engine)
    print(f"Found {results1['total']} tests\n")
    
    # Test 2: Run tests with additional files
    print("2. Running tests with additional external file:")
    results2 = InLaw.run_all(
        engine=engine, 
        inlaw_files=["test_inlaw_external.py"]
    )
    print(f"Found {results2['total']} tests\n")
    
    # Test 3: Run tests with directory import
    print("3. Running tests with directory import:")
    results3 = InLaw.run_all(
        engine=engine, 
        inlaw_dir="test_inlaw_dir"
    )
    print(f"Found {results3['total']} tests\n")
    
    # Test 4: Run tests with both files and directory
    print("4. Running tests with both external files and directory:")
    results4 = InLaw.run_all(
        engine=engine,
        inlaw_files=["test_inlaw_external.py"],
        inlaw_dir="test_inlaw_dir"
    )
    print(f"Found {results4['total']} tests\n")
    
    # Test 5: Demonstrate legacy compatibility
    print("5. Using legacy run_all_legacy method:")
    results5 = InLaw.run_all_legacy(
        engine,
        inlaw_files=["test_inlaw_external.py"]
    )
    print(f"Found {results5['total']} tests\n")
    
    print("=== Demonstration Complete ===")
    
    return results4


if __name__ == "__main__":
    main()
