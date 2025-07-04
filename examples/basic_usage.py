#!/usr/bin/env python3
"""
Basic usage example for plainerflow package
"""

# Example of importing plainerflow after manual path setup
# (for environments where pip is not available)
import sys
import os

# Add the parent directory to the path so we can import plainerflow
# In a real scenario, this would be the path to where plainerflow is located
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Now import plainerflow
import plainerflow

def main():
    """Demonstrate basic plainerflow usage"""
    print("=== plainerflow Basic Usage Example ===")
    
    # Show version
    print(f"plainerflow version: {plainerflow.__version__}")
    
    # Demonstrate that SQLAlchemy is available
    try:
        import sqlalchemy
        print(f"SQLAlchemy version: {sqlalchemy.__version__}")
        print("✓ SQLAlchemy dependency is working")
    except ImportError:
        print("✗ SQLAlchemy not available")
    
    print("\n=== Example completed ===")
    print("You can now add your own classes and functionality to plainerflow!")

if __name__ == "__main__":
    main()
