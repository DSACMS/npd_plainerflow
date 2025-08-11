import unittest
import sys
from pathlib import Path
from dynaconf import Dynaconf

# Add the parent directory to the path so we can import npd_plainerflow
sys.path.insert(0, str(Path(__file__).parent.parent))

from npd_plainerflow import CredentialFinder

class TestLoadConfig(unittest.TestCase):
    """Test cases for the load_config_from_env static method using test .env files."""

    def test_load_single_file(self):
        """Test loading a single, valid .env file."""
        settings = CredentialFinder.load_config_from_env(["test_a.env"])
        self.assertIsInstance(settings, Dynaconf)
        self.assertEqual(settings.DB_HOST, "localhost")
        self.assertEqual(settings.DB_PORT, '5432')
        self.assertEqual(settings.DB_USER, "a_user")
        self.assertEqual(settings.COMMON_VAR, "fileA")

    def test_load_multiple_files_override(self):
        """Test that later files override earlier ones."""
        settings = CredentialFinder.load_config_from_env(["test_a.env", "test_b.env"])
        self.assertIsInstance(settings, Dynaconf)
        
        # Values from test_b.env should override test_a.env
        self.assertEqual(settings.DB_HOST, "remotehost")
        self.assertEqual(settings.DB_USER, "admin")
        self.assertEqual(settings.COMMON_VAR, "fileB")
        
        # Value only present in test_a.env should still exist
        self.assertEqual(settings.DB_PORT, '5432')

if __name__ == '__main__':
    unittest.main()
