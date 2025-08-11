"""
ConfigNoir - A sophisticated configuration detection utility for PlainerFlow.

This module provides a robust way to detect and load configurations from various
sources and then uses EngineFetcher to create and attach a SQLAlchemy engine.
"""

import os
from pathlib import Path
from typing import Optional, List, Union, Dict, Any
from dynaconf import Dynaconf
from dotenv import dotenv_values

from npd_plainerflow.enginefetcher import EngineFetcher

class ConfigNoir:
    """
    A utility class for automatically detecting and loading database
    configurations from multiple sources.
    """

    @staticmethod
    def detect_and_load_config(
        *,
        config_files: Optional[List[Union[str, Path]]] = None,
        verbose: bool = False,
        sqlite_db_file: Optional[str] = None,
        password_worksheet: str = "DatawarehouseUP"
    ) -> Dynaconf:
        """
        Detects and loads configuration, returning a Dynaconf object with an
        eagerly created SQLAlchemy engine attached via EngineFetcher.

        Priority order for auto-detection:
        1. SQLite override (if sqlite_db_file is provided)
        2. Spark session (Databricks / PySpark)
        3. Google Colab + Drive secrets sheet
        4. Default .env file
        5. Fallback to a test database (PostgreSQL container or SQLite file)
        """
        settings = Dynaconf(envvar_prefix=False)
        settings._sql_alchemy_engine = None
        settings.database_connection_error_message = None

        is_explicit = bool(config_files) or bool(sqlite_db_file)

        # Scenario 1: Explicit .env files are provided
        if config_files:
            if not isinstance(config_files, list):
                raise TypeError("Expected 'config_files' to be a list of paths.")
            try:
                loaded_settings = ConfigNoir._load_from_files(config_files)
                settings.update(loaded_settings, merge=True) # type: ignore
            except Exception as e:
                settings.database_connection_error_message = f"Failed to load provided config files: {e}"
                if verbose:
                    print(f"[ConfigNoir] Error: {settings.database_connection_error_message}")
                return settings
        
        # Scenario 2: Auto-detection logic
        else:
            config_source = None
            if sqlite_db_file:
                config_source = ConfigNoir._get_sqlite_config(sqlite_db_file, verbose)
            if not config_source:
                config_source = ConfigNoir._try_spark_config(verbose)
            if not config_source:
                config_source = ConfigNoir._try_colab_config(password_worksheet, verbose)
            if not config_source:
                config_source = ConfigNoir._try_env_config(".env", verbose)
            if not config_source:
                config_source = ConfigNoir._get_testing_postgresql_config(verbose)

            if config_source:
                settings.update(config_source, merge=True) # type: ignore
            else:
                settings.database_connection_error_message = "Could not detect any valid configuration source."
                if verbose:
                    print(f"[ConfigNoir] Error: {settings.database_connection_error_message}")
                return settings

        # Use EngineFetcher to get the engine
        EngineFetcher.get_engine(settings, verbose=verbose, is_explicit_path=is_explicit)
        
        return settings

    @staticmethod
    def _load_from_files(config_files: List[Union[str, Path]]) -> Dynaconf:
        """Loads a Dynaconf object from an ordered list of .env files."""
        resolved = [Path(p).expanduser().resolve() for p in config_files]
        missing = [str(p) for p in resolved if not p.exists()]
        if missing:
            raise FileNotFoundError(f"Missing configuration file(s): {', '.join(missing)}")
        
        dirs = [str(p) for p in resolved if p.is_dir()]
        if dirs:
            raise IsADirectoryError(f"Expected file paths, but these are directories: {', '.join(dirs)}")

        settings = Dynaconf(envvar_prefix=False)
        for f in resolved:
            values = dotenv_values(f)
            settings.update(values, merge=True) # type: ignore
        return settings

    @staticmethod
    def _try_spark_config(verbose: bool) -> Optional[Dict[str, Any]]:
        """Attempts to get config from a Spark session."""
        try:
            from pyspark.sql import SparkSession # type: ignore
            spark = SparkSession.getActiveSession()
            if spark:
                jdbc_url = spark.conf.get("spark.databricks.jdbc.url", None)
                if jdbc_url:
                    if verbose:
                        print("[ConfigNoir] Using Spark session credentials.")
                    return {"DB_TYPE": "DATABRICKS", "JDBC_URL": jdbc_url}
        except (ImportError, Exception):
            return None
        return None

    @staticmethod
    def _try_colab_config(password_worksheet: str, verbose: bool) -> Optional[Dict[str, Any]]:
        """Attempts to get config from Google Colab."""
        try:
            import google.colab # type: ignore
            import gspread # type: ignore
            import pandas as pd
            from google.auth import default # type: ignore
            from google.colab import auth # type: ignore

            auth.authenticate_user()
            creds, _ = default()
            gc = gspread.authorize(creds)
            
            worksheet = gc.open(password_worksheet).sheet1
            rows = worksheet.get_all_values()
            
            if len(rows) > 10000:
                raise RuntimeError(f"Google Sheet has {len(rows)} rows, which exceeds the 10,000 row limit.")

            if verbose:
                print(f"[ConfigNoir] Using Google Colab credentials from sheet '{password_worksheet}'.")

            config_data = {row[0]: row[1] for row in rows if len(row) >= 2}
            
            if rows:
                headers = rows[0]
                if len(rows) > 1 and all(h in headers for h in ['username', 'password', 'server', 'port', 'database']):
                    first_row_data = dict(zip(headers, rows[1]))
                    config_data['GX_USERNAME'] = first_row_data.get('username')
                    config_data['GX_PASSWORD'] = first_row_data.get('password')
                    config_data['DB_HOST'] = first_row_data.get('server')
                    config_data['DB_PORT'] = first_row_data.get('port')
                    config_data['DB_DATABASE'] = first_row_data.get('database')
                    config_data['DB_TYPE'] = 'MYSQL'
            
            return config_data
        except (ImportError, Exception):
            return None

    @staticmethod
    def _try_env_config(env_path: str, verbose: bool) -> Optional[Dict[str, Any]]:
        """Attempts to get config from a .env file."""
        if not os.path.exists(env_path):
            return None
        try:
            if verbose:
                print(f"[ConfigNoir] Using .env file credentials from {env_path}.")
            return dotenv_values(env_path)
        except Exception:
            return None

    @staticmethod
    def _get_testing_postgresql_config(verbose: bool) -> Optional[Dict[str, Any]]:
        """Gets config for a testcontainers PostgreSQL instance."""
        try:
            from testcontainers.postgres import PostgreSqlContainer # type: ignore
            postgres_container = PostgreSqlContainer("postgres:13")
            postgres_container.start()
            connection_url = postgres_container.get_connection_url()
            if verbose:
                print(f"[ConfigNoir] Using testcontainers PostgreSQL database.")
            from sqlalchemy.engine.url import make_url
            url = make_url(connection_url)
            return {
                "DB_TYPE": "POSTGRESQL",
                "GX_USERNAME": url.username,
                "GX_PASSWORD": url.password,
                "DB_HOST": url.host,
                "DB_PORT": url.port,
                "DB_DATABASE": url.database
            }
        except (ImportError, Exception) as e:
            if verbose:
                print(f"[ConfigNoir] testcontainers not available or failed ({e}), falling back to SQLite.")
            return ConfigNoir._get_sqlite_config(str(Path.home() / "plainerflow_fallback.db"), verbose, is_fallback=True)

    @staticmethod
    def _get_sqlite_config(db_path: str, verbose: bool, is_fallback: bool = False) -> Dict[str, Any]:
        """Gets config for a SQLite database."""
        if db_path.startswith("~/"):
            db_path = str(Path.home() / db_path[2:])
        
        db_file = Path(db_path)
        db_file.parent.mkdir(parents=True, exist_ok=True)
        
        if verbose:
            message = "Falling back to" if is_fallback else "Using"
            print(f"[ConfigNoir] {message} local SQLite database: {db_path}")
            
        return {"DB_TYPE": "SQLITE", "DB_DATABASE": db_path}
