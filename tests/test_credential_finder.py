"""
Unit tests for CredentialFinder class.

Tests the various connection detection methods and fallback behavior.
"""

import unittest
import os
import tempfile
from pathlib import Path
from unittest.mock import patch, MagicMock
import sys

# Add the parent directory to the path so we can import npd_plainerflow
sys.path.insert(0, str(Path(__file__).parent.parent))

from npd_plainerflow import CredentialFinder
import sqlalchemy


class TestCredentialFinder(unittest.TestCase):
    """Test cases for CredentialFinder class."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        self.temp_db_path = os.path.join(self.temp_dir, "test.db")
    
    def tearDown(self):
        """Clean up test fixtures."""
        # Clean up any temporary files
        if os.path.exists(self.temp_db_path):
            os.remove(self.temp_db_path)
        
        # Clean up any .env files that might have been created
        env_file = os.path.join(self.temp_dir, ".env")
        if os.path.exists(env_file):
            os.remove(env_file)
        
        # Remove the temp directory
        try:
            os.rmdir(self.temp_dir)
        except OSError:
            # Directory might not be empty, remove contents first
            import shutil
            shutil.rmtree(self.temp_dir)
    
    def test_sqlite_override(self):
        """Test that sqlite_db_file parameter forces SQLite usage."""
        engine = CredentialFinder.detect_config(
            sqlite_db_file=self.temp_db_path,
            verbose=False
        )
        
        self.assertIsInstance(engine, sqlalchemy.engine.Engine)
        self.assertTrue(str(engine.url).startswith("sqlite:///"))
        self.assertIn(self.temp_db_path, str(engine.url))
        
        # Test that it actually works
        with engine.connect() as conn:
            from sqlalchemy import text
            result = conn.execute(text("SELECT 1"))
            row = result.fetchone()
            if row:
                self.assertEqual(row[0], 1)
    
    def test_sqlite_fallback(self):
        """Test SQLite fallback when no other methods work."""
        # This should fallback to SQLite since we're not in Spark/Colab
        # and no .env file exists
        engine = CredentialFinder.detect_config(
            env_path="/nonexistent/.env",
            verbose=False
        )
        
        self.assertIsInstance(engine, sqlalchemy.engine.Engine)
        self.assertTrue(str(engine.url).startswith("sqlite:///"))
        
        # Test that it works
        with engine.connect() as conn:
            from sqlalchemy import text
            result = conn.execute(text("SELECT 'fallback' as test"))
            row = result.fetchone()
            if row:
                self.assertEqual(row[0], "fallback")
    
    def test_env_file_missing(self):
        """Test behavior when .env file doesn't exist."""
        engine = CredentialFinder.detect_config(
            env_path="/path/that/does/not/exist/.env",
            verbose=False
        )
        
        # Should fallback to SQLite
        self.assertIsInstance(engine, sqlalchemy.engine.Engine)
        self.assertTrue(str(engine.url).startswith("sqlite:///"))
    
    def test_env_file_incomplete_credentials(self):
        """Test RuntimeError when .env file exists but has incomplete credentials."""
        # Create a temporary .env file with incomplete credentials
        env_file = os.path.join(self.temp_dir, ".env")
        with open(env_file, "w") as f:
            f.write("GX_USERNAME=testuser\n")
            f.write("# Missing other required variables\n")
        
        with self.assertRaises(RuntimeError) as context:
            CredentialFinder.detect_config(env_path=env_file, verbose=False)
        
        self.assertIn("Incomplete .env credentials", str(context.exception))
        self.assertIn("GX_PASSWORD", str(context.exception))
    
    def test_spark_not_available(self):
        """Test behavior when pyspark is not available."""
        # Since pyspark is not installed in our test environment,
        # this should naturally fallback to SQLite
        engine = CredentialFinder.detect_config(verbose=False)
        
        # Should fallback to SQLite
        self.assertIsInstance(engine, sqlalchemy.engine.Engine)
        self.assertTrue(str(engine.url).startswith("sqlite:///"))
    
    def test_colab_not_available(self):
        """Test behavior when google.colab is not available."""
        # Since google.colab is not installed in our test environment,
        # this should naturally fallback to SQLite
        engine = CredentialFinder.detect_config(verbose=False)
        
        # Should fallback to SQLite
        self.assertIsInstance(engine, sqlalchemy.engine.Engine)
        self.assertTrue(str(engine.url).startswith("sqlite:///"))
    
    def test_verbose_output(self):
        """Test that verbose mode produces output."""
        import io
        import contextlib
        
        # Capture stdout
        f = io.StringIO()
        with contextlib.redirect_stdout(f):
            engine = CredentialFinder.detect_config(
                sqlite_db_file=self.temp_db_path,
                verbose=True
            )
        
        output = f.getvalue()
        self.assertIn("[CredentialFinder]", output)
        self.assertIn("SQLite database", output)
    
    def test_quiet_mode(self):
        """Test that quiet mode produces no output."""
        import io
        import contextlib
        
        # Capture stdout
        f = io.StringIO()
        with contextlib.redirect_stdout(f):
            engine = CredentialFinder.detect_config(
                sqlite_db_file=self.temp_db_path,
                verbose=False
            )
        
        output = f.getvalue()
        self.assertEqual(output, "")
    
    def test_home_directory_expansion(self):
        """Test that ~ in sqlite_db_file gets expanded to home directory."""
        engine = CredentialFinder.detect_config(
            sqlite_db_file="~/test_npd_plainerflow.db",
            verbose=False
        )
        
        self.assertIsInstance(engine, sqlalchemy.engine.Engine)
        # The URL should contain the expanded home directory path
        url_str = str(engine.url)
        self.assertTrue(url_str.startswith("sqlite:///"))
        self.assertNotIn("~", url_str)  # ~ should be expanded
        
        # Clean up the created file
        home_db_path = str(Path.home() / "test_npd_plainerflow.db")
        if os.path.exists(home_db_path):
            os.remove(home_db_path)
    
    def test_env_path_none_skips_env_lookup(self):
        """Test that env_path=None skips .env file lookup."""
        # This should skip .env lookup and go straight to SQLite fallback
        engine = CredentialFinder.detect_config(
            env_path=None,
            verbose=False
        )
        
        self.assertIsInstance(engine, sqlalchemy.engine.Engine)
        self.assertTrue(str(engine.url).startswith("sqlite:///"))


class TestCredentialFinderIntegration(unittest.TestCase):
    """Integration tests for CredentialFinder."""
    
    def test_complete_workflow_with_env_file(self):
        """Test complete workflow with a properly configured .env file."""
        # Create a temporary .env file with complete credentials
        with tempfile.NamedTemporaryFile(mode='w', suffix='.env', delete=False) as f:
            f.write("GX_USERNAME=testuser\n")
            f.write("GX_PASSWORD=testpass\n")
            f.write("DB_DATABASE=testdb\n")
            f.write("DB_PORT=3306\n")
            f.write("DB_HOST=localhost\n")
            env_file_path = f.name
        
        try:
            # This should try to connect to MySQL and likely fail, but we'll catch any exception
            # The important thing is that it should not raise a RuntimeError about incomplete credentials
            try:
                engine = CredentialFinder.detect_config(env_path=env_file_path, verbose=False)
                # If we get here, it means the connection attempt succeeded or fell back to SQLite
                # Either way, it means the credentials were complete
                self.assertIsInstance(engine, sqlalchemy.engine.Engine)
            except RuntimeError as e:
                # If we get a RuntimeError, it should NOT be about incomplete credentials
                self.assertNotIn("Incomplete .env credentials", str(e))
                # Re-raise if it's a different RuntimeError
                if "Incomplete .env credentials" not in str(e):
                    raise
            
        finally:
            # Clean up
            os.unlink(env_file_path)


if __name__ == '__main__':
    unittest.main()
