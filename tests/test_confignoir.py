"""
Unit tests for ConfigNoir and EngineFetcher classes.
"""

import unittest
import os
import tempfile
from pathlib import Path
import sys
from unittest.mock import patch, MagicMock

# Add the parent directory to the path so we can import the necessary modules
sys.path.insert(0, str(Path(__file__).parent.parent))

from npd_plainerflow.confignoir import ConfigNoir
from npd_plainerflow.enginefetcher import EngineFetcher
from dynaconf import Dynaconf
import sqlalchemy

class TestConfigNoir(unittest.TestCase):
    """Test cases for ConfigNoir class."""

    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        self.temp_db_path = os.path.join(self.temp_dir, "test.db")

    def tearDown(self):
        """Clean up test fixtures."""
        import shutil
        shutil.rmtree(self.temp_dir)

    def test_sqlite_override(self):
        """Test that sqlite_db_file parameter forces SQLite usage."""
        settings = ConfigNoir.detect_and_load_config(
            sqlite_db_file=self.temp_db_path,
            verbose=False
        )
        self.assertIsInstance(settings, Dynaconf)
        self.assertIsNotNone(settings._sql_alchemy_engine)
        self.assertIsNone(settings.database_connection_error_message)
        self.assertIsInstance(settings._sql_alchemy_engine, sqlalchemy.engine.Engine)
        self.assertTrue(str(settings._sql_alchemy_engine.url).startswith("sqlite:///")) # type: ignore

    def test_fallback_behavior(self):
        """Test fallback when no other methods work."""
        # With no config files, it should fall back to the testcontainer/sqlite DB
        settings = ConfigNoir.detect_and_load_config(
            config_files=None,
            verbose=False
        )
        self.assertIsNotNone(settings._sql_alchemy_engine)
        self.assertIsNone(settings.database_connection_error_message)

    def test_explicit_env_file_success(self):
        """Test loading a valid .env file."""
        env_file = os.path.join(self.temp_dir, ".env")
        with open(env_file, "w") as f:
            f.write("DB_TYPE=SQLITE\n")
            f.write(f"DB_DATABASE={self.temp_db_path}\n")

        settings = ConfigNoir.detect_and_load_config(
            config_files=[env_file],
            verbose=False
        )
        self.assertIsNotNone(settings._sql_alchemy_engine)
        self.assertIsNone(settings.database_connection_error_message)
        self.assertTrue(str(settings._sql_alchemy_engine.url).startswith("sqlite:///")) # type: ignore

    def test_explicit_env_file_connection_failure_raises_error(self):
        """Test that a connection failure with an explicit .env file raises a RuntimeError."""
        env_file = os.path.join(self.temp_dir, ".env")
        with open(env_file, "w") as f:
            f.write("DB_TYPE=POSTGRESQL\n")
            f.write("DB_HOST=nonexistent.host\n")
            f.write("DB_PORT=5432\n")
            f.write("DB_USER=user\n")
            f.write("DB_PASSWORD=pass\n")
            f.write("DB_DATABASE=db\n")

        with self.assertRaises(RuntimeError):
            ConfigNoir.detect_and_load_config(
                config_files=[env_file],
                verbose=False
            )

    def test_missing_explicit_file_returns_error_message(self):
        """Test that a missing explicit file returns an object with an error message."""
        settings = ConfigNoir.detect_and_load_config(
            config_files=["/non/existent/file.env"],
            verbose=False
        )
        self.assertIsNone(settings._sql_alchemy_engine)
        self.assertIsNotNone(settings.database_connection_error_message)
        self.assertIn("Missing configuration file(s)", settings.database_connection_error_message) # type: ignore

    def test_incomplete_credentials_in_env_file(self):
        """Test that incomplete credentials in an explicit .env file raises a RuntimeError."""
        env_file = os.path.join(self.temp_dir, ".env")
        with open(env_file, "w") as f:
            f.write("DB_TYPE=POSTGRESQL\n")
            f.write("DB_HOST=localhost\n")
            # Missing other required variables

        with self.assertRaises(RuntimeError) as context:
            ConfigNoir.detect_and_load_config(
                config_files=[env_file],
                verbose=False
            )
        self.assertIn("Missing required database credential", str(context.exception))

    @patch.dict(os.environ, {
        'DB_TYPE': 'SQLITE',
        'DB_DATABASE': '/tmp/test_env_vars.db'
    }, clear=False)
    def test_environment_variables_sqlite_success(self):
        """Test that environment variables work for SQLite when no .env file exists."""
        settings = ConfigNoir.detect_and_load_config(verbose=False)
        self.assertIsNotNone(settings._sql_alchemy_engine)
        self.assertIsNone(settings.database_connection_error_message)
        self.assertTrue(str(settings._sql_alchemy_engine.url).startswith("sqlite:///")) # type: ignore

    @patch.dict(os.environ, {
        'DB_TYPE': 'POSTGRESQL',
        'DB_HOST': 'localhost',
        'DB_USER': 'test_user',
        'DB_PASSWORD': 'test_pass',
        'DB_PORT': '5432',
        'DB_DATABASE': 'test_db'
    }, clear=False)
    def test_environment_variables_postgresql_precedence_over_env_file(self):
        """Test that environment variables take precedence over .env files."""
        # Create .env file with different SQLite config
        env_file = os.path.join(self.temp_dir, ".env")
        with open(env_file, "w") as f:
            f.write("DB_TYPE=SQLITE\n")
            f.write(f"DB_DATABASE={self.temp_db_path}\n")

        # Change to temp directory to make .env file discoverable
        original_cwd = os.getcwd()
        try:
            os.chdir(self.temp_dir)
            # Environment variables should take precedence, but connection will fail
            # because we're using fake PostgreSQL credentials
            settings = ConfigNoir.detect_and_load_config(verbose=False)
            # Since the PostgreSQL connection will fail, it should fall back
            # Check that it didn't use the SQLite config from .env file by checking the error
            if settings.database_connection_error_message:
                # If there's an error, it means it tried the env vars (PostgreSQL) first
                db_type = getattr(settings, 'DB_TYPE', '')
                self.assertIn("POSTGRESQL", str(db_type).upper())
            else:
                # If no error, it should be using environment variables (though connection might work in test environment)
                self.assertEqual(getattr(settings, 'DB_TYPE', None), 'POSTGRESQL')
        finally:
            os.chdir(original_cwd)

    @patch.dict(os.environ, {
        'DB_TYPE': 'POSTGRESQL',
        'DB_HOST': 'localhost'
        # Missing other required variables
    }, clear=False)
    def test_incomplete_environment_variables_fallback_to_env_file(self):
        """Test that incomplete environment variables fall back to .env file."""
        # Create valid .env file
        env_file = os.path.join(self.temp_dir, ".env")
        with open(env_file, "w") as f:
            f.write("DB_TYPE=SQLITE\n")
            f.write(f"DB_DATABASE={self.temp_db_path}\n")

        original_cwd = os.getcwd()
        try:
            os.chdir(self.temp_dir)
            settings = ConfigNoir.detect_and_load_config(verbose=False)
            # Should fall back to .env file (SQLite)
            self.assertIsNotNone(settings._sql_alchemy_engine)
            self.assertTrue(str(settings._sql_alchemy_engine.url).startswith("sqlite:///")) # type: ignore
        finally:
            os.chdir(original_cwd)

    @patch.dict(os.environ, {
        'DB_TYPE': 'SQLITE'
        # Missing DB_DATABASE
    }, clear=False)
    def test_incomplete_sqlite_environment_variables(self):
        """Test that incomplete SQLite environment variables fall back to next option."""
        settings = ConfigNoir.detect_and_load_config(verbose=False)
        # Should fall back to test database since SQLite env vars are incomplete
        self.assertIsNotNone(settings._sql_alchemy_engine)
        # Should not use the incomplete environment variables

    def test_no_environment_variables_uses_fallback(self):
        """Test that when no relevant environment variables are set, it uses fallback."""
        # Ensure no DB environment variables are set for this test
        env_vars_to_clear = ['DB_TYPE', 'DB_HOST', 'DB_USER', 'DB_PASSWORD', 'DB_PORT', 'DB_DATABASE']
        with patch.dict(os.environ, {}, clear=False):
            # Remove any existing DB env vars
            for var in env_vars_to_clear:
                os.environ.pop(var, None)
            
            settings = ConfigNoir.detect_and_load_config(verbose=False)
            # Should fall back to test database
            self.assertIsNotNone(settings._sql_alchemy_engine)

    @patch.dict(os.environ, {'DB_TYPE': 'MYSQL'}, clear=False)  
    def test_try_environment_variables_method_directly(self):
        """Test the _try_environment_variables method directly with various scenarios."""
        # Test with only DB_TYPE - should return None for non-SQLite
        result = ConfigNoir._try_environment_variables(verbose=False)
        self.assertIsNone(result)

    @patch.dict(os.environ, {
        'DB_TYPE': 'MYSQL',
        'DB_HOST': 'localhost',
        'DB_USER': 'user',
        'DB_DATABASE': 'mydb'
    }, clear=False)
    def test_try_environment_variables_mysql_complete(self):
        """Test _try_environment_variables with complete MySQL configuration."""
        result = ConfigNoir._try_environment_variables(verbose=False)
        self.assertIsNotNone(result)
        self.assertEqual(result['DB_TYPE'], 'MYSQL') # type: ignore
        self.assertEqual(result['DB_HOST'], 'localhost') # type: ignore
        self.assertEqual(result['DB_USER'], 'user') # type: ignore
        self.assertEqual(result['DB_DATABASE'], 'mydb') # type: ignore

if __name__ == '__main__':
    unittest.main()
