import sys
from pathlib import Path

from npd_plainerflow import CredentialFinder

print("--- Loading single file ---")
settings1 = CredentialFinder.load_config_from_env(["test_a.env"])
print(f"DB_HOST: {settings1.DB_HOST}")
print(f"DB_PORT: {settings1.DB_PORT}")
print(f"DB_USER: {settings1.DB_USER}")
print(f"COMMON_VAR: {settings1.COMMON_VAR}")

print("\n--- Loading multiple files (override) ---")
settings2 = CredentialFinder.load_config_from_env(["test_a.env", "test_b.env"])
print(f"DB_HOST: {settings2.DB_HOST}")
print(f"DB_PORT: {settings2.get('DB_PORT', 'Not Found')}")
print(f"DB_USER: {settings2.DB_USER}")
print(f"COMMON_VAR: {settings2.COMMON_VAR}")
