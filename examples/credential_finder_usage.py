#!/usr/bin/env python3
"""
Example usage of CredentialFinder in different scenarios.

This demonstrates how to use CredentialFinder in various environments
and how it gracefully falls back through different connection methods.
"""

import sys
from pathlib import Path

# Add the parent directory to the path so we can import plainerflow
sys.path.insert(0, str(Path(__file__).parent.parent))

from plainerflow import CredentialFinder


def example_basic_usage():
    """Basic usage example - let CredentialFinder auto-detect."""
    print("=== Basic Usage Example ===")
    
    # Simple usage - CredentialFinder will try all methods and fallback to SQLite
    engine = CredentialFinder.detect_config(verbose=True)
    
    # Use the engine for database operations
    with engine.connect() as conn:
        from sqlalchemy import text
        
        # Create a simple table
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                email TEXT UNIQUE
            )
        """))
        
        # Insert some data
        conn.execute(text("""
            INSERT OR REPLACE INTO users (id, name, email) 
            VALUES (1, 'John Doe', 'john@example.com')
        """))
        
        # Query the data
        result = conn.execute(text("SELECT * FROM users"))
        users = result.fetchall()
        
        print(f"Users in database: {users}")
        
        # Commit the transaction
        conn.commit()
    
    print("✓ Basic usage example completed\n")


def example_custom_sqlite():
    """Example using a custom SQLite database file."""
    print("=== Custom SQLite Example ===")
    
    # Force SQLite usage with a custom file
    custom_db_path = "/tmp/my_project.db"
    engine = CredentialFinder.detect_config(
        sqlite_db_file=custom_db_path,
        verbose=True
    )
    
    with engine.connect() as conn:
        from sqlalchemy import text
        
        # Create a project-specific table
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS projects (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                status TEXT DEFAULT 'active'
            )
        """))
        
        # Insert project data
        conn.execute(text("""
            INSERT OR REPLACE INTO projects (id, name, status) 
            VALUES (1, 'PlainerFlow', 'development')
        """))
        
        # Query projects
        result = conn.execute(text("SELECT * FROM projects"))
        projects = result.fetchall()
        
        print(f"Projects: {projects}")
        conn.commit()
    
    print("✓ Custom SQLite example completed\n")


def example_env_file_simulation():
    """Example showing what happens when .env file is missing."""
    print("=== .env File Missing Example ===")
    
    # Try to use a non-existent .env file - should fallback to SQLite
    engine = CredentialFinder.detect_config(
        env_path="/path/that/does/not/exist/.env",
        verbose=True
    )
    
    print(f"Engine type: {type(engine)}")
    print(f"Connection URL: {engine.url}")
    
    # Verify it works
    with engine.connect() as conn:
        from sqlalchemy import text
        result = conn.execute(text("SELECT 'fallback working' as message"))
        message = result.fetchone()
        print(f"Test message: {message[0]}")
    
    print("✓ .env file missing example completed\n")


def example_verbose_vs_quiet():
    """Example showing verbose vs quiet modes."""
    print("=== Verbose vs Quiet Example ===")
    
    print("Quiet mode (verbose=False):")
    engine1 = CredentialFinder.detect_config(verbose=False)
    print(f"Got engine: {type(engine1).__name__}")
    
    print("\nVerbose mode (verbose=True):")
    engine2 = CredentialFinder.detect_config(verbose=True)
    print(f"Got engine: {type(engine2).__name__}")
    
    print("✓ Verbose vs quiet example completed\n")


def main():
    """Run all examples."""
    print("PlainerFlow CredentialFinder Usage Examples\n")
    print("=" * 50)
    
    try:
        example_basic_usage()
        example_custom_sqlite()
        example_env_file_simulation()
        example_verbose_vs_quiet()
        
        print("🎉 All examples completed successfully!")
        print("\nNext steps:")
        print("- Try creating a .env file with database credentials")
        print("- Test in Google Colab environment")
        print("- Test with Spark/Databricks")
        
    except Exception as e:
        print(f"❌ Example failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
