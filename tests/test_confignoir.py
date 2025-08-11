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
            f.write("GX_USERNAME=user\n")
            f.write("GX_PASSWORD=pass\n")
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

if __name__ == '__main__':
    unittest.main()
