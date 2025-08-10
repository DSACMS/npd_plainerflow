"""
CredentialFinder - A lightweight connection discovery utility for PlainerFlow.

Inspects the runtime environment in priority order and returns a ready-to-use 
SQLAlchemy Engine, falling back to local SQLite so pipelines always have 
something to run against.
"""

import os
import sys
from pathlib import Path
from typing import Optional
import sqlalchemy
from sqlalchemy import create_engine


class CredentialFinder:
    """
    A utility class for automatically detecting and configuring database connections
    across different environments (Spark, Google Colab, .env files, SQLite fallback).
    """
    
    @staticmethod
    def detect_config(
        *, 
        env_path: Optional[str] = ".env", 
        verbose: bool = False,
        sqlite_db_file: Optional[str] = None,
        password_worksheet: str = "DatawarehouseUP"
    ) -> sqlalchemy.engine.Engine:
        """
        Detects the best available connection source, in order of precedence,
        and returns a SQLAlchemy Engine.

        Priority order:
        1. SQLite override (if sqlite_db_file is provided)
        2. Spark session (Databricks / PySpark)
        3. Google Colab + Drive secrets sheet
        4. .env file (traditional secrets)
        5. SQLite fallback

        Parameters
        ----------
        env_path : str | None
            Custom path to a .env file. If None, .env lookup is skipped.
        verbose : bool
            If True, print which source was chosen.
        sqlite_db_file : str | None
            If provided, forces SQLite usage with this file path, bypassing all other detection.
        password_worksheet : str
            Name of the Google Drive worksheet containing credentials (for Colab mode).

        Returns
        -------
        sqlalchemy.engine.Engine
            A configured SQLAlchemy engine ready for use.

        Raises
        ------
        RuntimeError
            If dependencies for a detected source are missing or credentials are incomplete.
        """
        
        # Priority 1: SQLite Override
        if sqlite_db_file is not None:
            return CredentialFinder._create_sqlite_engine(sqlite_db_file, verbose)
        
        # Priority 2: Spark Session
        try:
            engine = CredentialFinder._try_spark_connection(verbose)
            if engine is not None:
                return engine
        except RuntimeError:
            raise  # Re-raise RuntimeError for missing dependencies
        except Exception:
            pass  # Continue to next priority
        
        # Priority 3: Google Colab
        try:
            engine = CredentialFinder._try_colab_connection(password_worksheet, verbose)
            if engine is not None:
                return engine
        except RuntimeError:
            raise  # Re-raise RuntimeError for missing dependencies
        except Exception:
            pass  # Continue to next priority
        
        # Priority 4: .env File
        if env_path is not None:
            try:
                engine = CredentialFinder._try_env_connection(env_path, verbose)
                if engine is not None:
                    return engine
            except RuntimeError:
                raise  # Re-raise RuntimeError for incomplete credentials
            except Exception:
                pass  # Continue to fallback
        
        # Priority 5: PostgreSQL Testing Fallback
        return CredentialFinder._create_testing_postgresql_engine(verbose)
    
    @staticmethod
    def _try_spark_connection(verbose: bool) -> Optional[sqlalchemy.engine.Engine]:
        """
        Attempt to detect and connect via Spark session.
        
        Returns None if Spark is not available.
        Raises RuntimeError if Spark is detected but dependencies are missing.
        """
        try:
            # Check if pyspark is available
            import pyspark # type: ignore
            from pyspark.sql import SparkSession # type: ignore
        except ImportError:
            return None  # Spark not available, continue to next priority
        
        try:
            # Check for active Spark session
            spark = SparkSession.getActiveSession()
            if spark is None:
                return None  # No active session, continue to next priority
            
            # Try to get Databricks JDBC URL from Spark config
            spark_conf = spark.conf
            jdbc_url = None
            
            try:
                jdbc_url = spark_conf.get("spark.databricks.jdbc.url")
            except Exception:
                pass
            
            if jdbc_url:
                if verbose:
                    print("[CredentialFinder] Using Spark session credentials.")
                return create_engine(jdbc_url)
            else:
                # Build a databricks connector URL if no JDBC URL found
                # This is a simplified approach - in practice, you'd need more config
                if verbose:
                    print("[CredentialFinder] Using Spark session credentials (databricks connector).")
                # For now, we'll fall through to next priority if no JDBC URL is configured
                return None
                
        except Exception as e:
            # If we have pyspark but can't connect, that's a configuration issue
            raise RuntimeError(f"Spark session detected but connection failed: {str(e)}")
    
    @staticmethod
    def _try_colab_connection(password_worksheet: str, verbose: bool) -> Optional[sqlalchemy.engine.Engine]:
        """
        Attempt to detect and connect via Google Colab credentials.
        
        Returns None if not in Colab environment.
        Raises RuntimeError if in Colab but dependencies are missing.
        """
        try:
            # Check if we're in Google Colab
            import google.colab # type: ignore
        except ImportError:
            return None  # Not in Colab, continue to next priority
        
        try:
            # Import required Google API dependencies
            import gspread # type: ignore
            import pandas as pd
            from google.auth import default # type: ignore
            from google.colab import auth # type: ignore
        except ImportError as e:
            raise RuntimeError(
                f"Google Colab detected but required dependencies missing: {str(e)}. "
                "Install with: pip install gspread google-auth pandas"
            )
        
        try:
            # Authenticate with Google
            auth.authenticate_user()
            creds, _ = default()
            gc = gspread.authorize(creds)
            
            # Open the specified worksheet
            worksheet = gc.open(password_worksheet).sheet1
            rows = worksheet.get_all_values()
            df = pd.DataFrame(rows)
            
            # Convert first row to column headers
            df.columns = df.iloc[0]
            df = df.iloc[1:]
            
            # Extract credentials from first data row
            username = df.iat[0, 0]
            password = df.iat[0, 1]
            server = df.iat[0, 2]
            port = str(df.iat[0, 3])
            database = df.iat[0, 4]
            
            # Build MySQL connection string
            sql_url = f"mysql+pymysql://{username}:{password}@{server}:{port}/{database}"
            
            if verbose:
                print("[CredentialFinder] Using Google Colab credentials.")
            
            return create_engine(sql_url)
            
        except Exception as e:
            raise RuntimeError(f"Google Colab credentials access failed: {str(e)}")
    
    @staticmethod
    def _create_testing_postgresql_engine(verbose: bool) -> sqlalchemy.engine.Engine:
        """
        Create a testing PostgreSQL engine using testcontainers.
        
        Parameters
        ----------
        verbose : bool
            Whether to print verbose output.
        """
        try:
            from testcontainers.postgres import PostgreSqlContainer # type: ignore
        except ImportError:
            # Fall back to SQLite if testcontainers is not available
            if verbose:
                print("[CredentialFinder] testcontainers not available, falling back to SQLite")
            fallback_path = str(Path.home() / "plainerflow_fallback.db")
            return CredentialFinder._create_sqlite_engine(fallback_path, verbose, is_fallback=True)
        
        try:
            # Create a PostgreSQL container and store it globally to keep it alive
            postgres_container = PostgreSqlContainer("postgres:13")
            postgres_container.start()
            
            # Store the container globally so it doesn't get garbage collected
            CredentialFinder._postgres_container = postgres_container
            
            # Get the connection URL
            connection_url = postgres_container.get_connection_url()
            
            if verbose:
                print(f"[CredentialFinder] Using testcontainers PostgreSQL database: {connection_url}")
            
            return create_engine(connection_url)
            
        except Exception as e:
            # Fall back to SQLite if PostgreSQL setup fails
            if verbose:
                print(f"[CredentialFinder] PostgreSQL container setup failed ({str(e)}), falling back to SQLite")
            fallback_path = str(Path.home() / "plainerflow_fallback.db")
            return CredentialFinder._create_sqlite_engine(fallback_path, verbose, is_fallback=True)
    
    @staticmethod
    def _try_env_connection(env_path: str, verbose: bool) -> Optional[sqlalchemy.engine.Engine]:
        """
        Attempt to connect using .env file credentials.
        
        Returns None if .env file doesn't exist.
        Raises RuntimeError if .env exists but credentials are incomplete.
        """

        is_debug = True

        if not os.path.exists(env_path):
            return None  # .env file doesn't exist, continue to fallback
        
        try:
            from dotenv import load_dotenv
        except ImportError:
            raise RuntimeError(
                f".env file found at {env_path} but python-dotenv not installed. "
                "Install with: pip install python-dotenv"
            )
        
        # Load environment variables
        load_dotenv(env_path)
        
        # Check for required credentials
        required_vars = ['GX_USERNAME', 'GX_PASSWORD', 'DB_DATABASE', 'DB_PORT', 'DB_HOST']
        missing_vars = []
        
        for var in required_vars:
            if not os.getenv(var):
                missing_vars.append(var)
        
        if missing_vars:
            raise RuntimeError(
                f"Incomplete .env credentials. Missing variables: {', '.join(missing_vars)}"
            )
        
        # Build connection string
        username = os.getenv('GX_USERNAME')
        password = os.getenv('GX_PASSWORD')
        database = os.getenv('DB_DATABASE')
        port = os.getenv('DB_PORT')
        host = os.getenv('DB_HOST')
        
        database_type = os.getenv('DB_TYPE', 'MYSQL').upper()  # Default to MYSQL if not specified

        if(is_debug):
            print(f"CredentialFinder: Detected DB_TYPE of {database_type}")

        # Build connection string based on database type
        if database_type == 'POSTGRESQL':
            sql_url = f"postgresql+psycopg2://{username}:{password}@{host}:{port}/{database}"
        elif database_type == 'MYSQL':
            sql_url = f"mysql+pymysql://{username}:{password}@{host}:{port}/{database}"
        elif database_type == 'SQLITE':
            # For SQLite, we only need the database path (ignore host/port/username/password)
            sqlite_path = database if database else 'plainerflow.db'
            sql_url = f"sqlite:///{sqlite_path}"
        else:
            # For other database types, try a generic approach
            # This covers databases like Oracle, SQL Server, etc.
            driver_mapping = {
                'ORACLE': 'oracle+cx_oracle',
                'SQLSERVER': 'mssql+pyodbc',
                'MSSQL': 'mssql+pyodbc'
            }
            driver = driver_mapping.get(database_type, database_type.lower())
            sql_url = f"{driver}://{username}:{password}@{host}:{port}/{database}"
        
        if verbose:
            print(f"[CredentialFinder] Using .env file credentials from {env_path}.")
        
        try:
            engine = create_engine(sql_url)
            # Test the connection
            with engine.connect() as connection:
                if verbose:
                    print("[CredentialFinder] Database connection successful.")
            return engine
        except Exception as e:
            raise RuntimeError(
                f"Failed to connect to the database using credentials from {env_path}. "
                f"Please check your .env file and ensure the database is running. "
                f"Original error: {e}"
            )
    
    @staticmethod
    def _create_sqlite_engine(db_path: str, verbose: bool, is_fallback: bool = False) -> sqlalchemy.engine.Engine:
        """
        Create a SQLite engine with the specified database path.
        
        Parameters
        ----------
        db_path : str
            Path to the SQLite database file.
        verbose : bool
            Whether to print verbose output.
        is_fallback : bool
            Whether this is being used as a fallback (affects verbose message).
        """
        # Expand user home directory if needed
        if db_path.startswith("~/"):
            db_path = str(Path.home() / db_path[2:])
        
        # Ensure parent directory exists
        db_file = Path(db_path)
        db_file.parent.mkdir(parents=True, exist_ok=True)
        
        sql_url = f"sqlite:///{db_path}"
        
        if verbose:
            if is_fallback:
                print(f"[CredentialFinder] Falling back to local SQLite database: {db_path}")
            else:
                print(f"[CredentialFinder] Using SQLite database: {db_path}")
        
        return create_engine(sql_url)
