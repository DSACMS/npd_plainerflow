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
    
    @patch('npd_plainerflow.credential_finder.create_engine')
    def test_complete_workflow_with_env_file(self, mock_create_engine):
        """Test complete workflow with a properly configured .env file."""
        # Mock the create_engine to avoid actual DB connection
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine

        # Create a temporary .env file with complete credentials
        with tempfile.NamedTemporaryFile(mode='w', suffix='.env', delete=False) as f:
            f.write("GX_USERNAME=testuser\n")
            f.write("GX_PASSWORD=testpass\n")
            f.write("DB_DATABASE=testdb\n")
            f.write("DB_PORT=3306\n")
            f.write("DB_HOST=localhost\n")
            env_file_path = f.name
        
        try:
            # This should now call the mocked create_engine
            engine = CredentialFinder.detect_config(env_path=env_file_path, verbose=False)
            
            # Check that create_engine was called with the correct URL
            mock_create_engine.assert_called_once()
            self.assertEqual(engine, mock_engine)

        finally:
            # Clean up
            os.unlink(env_file_path)




class TestLoadConfigFromEnv(unittest.TestCase):
    """Test cases for the load_config_from_env static method."""

    def setUp(self):
        """Set up temporary .env files for testing."""
        self.temp_dir = tempfile.mkdtemp()
        self.env_file1_path = os.path.join(self.temp_dir, "test1.env")
        self.env_file2_path = os.path.join(self.temp_dir, "test2.env")

        with open(self.env_file1_path, "w") as f:
            f.write("DB_HOST=localhost\n")
            f.write("DB_PORT=5432\n")
            f.write("COMMON_VAR=file1\n")
            f.write("DB_USER=test\n")

        with open(self.env_file2_path, "w") as f:
            f.write("DB_USER=admin\n")
            f.write("COMMON_VAR=file2\n")
            f.write("DB_HOST=remotehost\n")

    def tearDown(self):
        """Clean up temporary files and directory."""
        import shutil
        shutil.rmtree(self.temp_dir)

    def test_load_single_file(self):
        """Test loading a single, valid .env file."""
        settings = CredentialFinder.load_config_from_env([self.env_file1_path])
        from dynaconf import Dynaconf
        self.assertIsInstance(settings, Dynaconf)
        self.assertEqual(settings.DB_HOST, "localhost")
        self.assertEqual(settings.DB_PORT, "5432")

    def test_load_multiple_files_override(self):
        """Test that later files override earlier ones when duplicates are allowed."""
        settings = CredentialFinder.load_config_from_env(
            [self.env_file1_path, self.env_file2_path]
        )
        self.assertEqual(settings.DB_HOST, "remotehost")
        self.assertEqual(settings.DB_USER, "admin")
        self.assertEqual(settings.COMMON_VAR, "file2")

    def test_missing_file_raises_error(self):
        """Test that a RuntimeError is raised if a configuration file is missing."""
        with self.assertRaises(RuntimeError) as context:
            CredentialFinder.load_config_from_env(["/non/existent/file.env"])
        self.assertIn("Missing configuration file(s)", str(context.exception))

    def test_directory_as_file_raises_error(self):
        """Test that a RuntimeError is raised if a path is a directory."""
        with self.assertRaises(RuntimeError) as context:
            CredentialFinder.load_config_from_env([self.temp_dir])
        self.assertIn("Expected file paths, but these are directories", str(context.exception))

    def test_empty_file_list_raises_error(self):
        """Test that a RuntimeError is raised if the file list is empty."""
        with self.assertRaises(RuntimeError) as context:
            CredentialFinder.load_config_from_env([])
        self.assertIn("No files provided", str(context.exception))

    def test_non_list_input_raises_error(self):
        """Test that a TypeError is raised if the input is not a list."""
        with self.assertRaises(TypeError):
            CredentialFinder.load_config_from_env("not_a_list")


if __name__ == '__main__':
    unittest.main()
