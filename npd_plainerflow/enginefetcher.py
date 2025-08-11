"""
EngineFetcher - A utility for creating SQLAlchemy engines from Dynaconf settings.
"""

from sqlalchemy import create_engine
from dynaconf import Dynaconf
import sqlalchemy

class EngineFetcher:
    """
    A utility class responsible for creating a SQLAlchemy engine from a given
    Dynaconf configuration object.
    """

    @staticmethod
    def get_engine(
        settings: Dynaconf,
        *,
        verbose: bool = False,
        is_explicit_path: bool = False
    ) -> None:
        """
        Creates a SQLAlchemy engine and attaches it to the settings object.

        This method populates `_sql_alchemy_engine` and `database_connection_error_message`
        on the provided settings object.

        Parameters
        ----------
        settings : dynaconf.Dynaconf
            The configuration object containing database credentials.
        verbose : bool
            If True, print detailed output.
        is_explicit_path : bool
            If True, raise a hard RuntimeError on connection failure.
        """
        try:
            sql_url = EngineFetcher._build_sql_url(settings)
            if verbose:
                # Avoid printing credentials in the URL
                safe_url_parts = sql_url.split('://', 1)
                if '@' in safe_url_parts[1]:
                    _, rest = safe_url_parts[1].split('@', 1)
                    print(f"[EngineFetcher] Attempting to connect with URL: {safe_url_parts[0]}://...@{rest}")
                else:
                    print(f"[EngineFetcher] Attempting to connect with URL: {sql_url}")

            engine = create_engine(sql_url)
            with engine.connect() as connection:
                if verbose:
                    print("[EngineFetcher] Database connection successful.")
            settings._sql_alchemy_engine = engine
        except Exception as e:
            error_msg = f"Failed to create SQLAlchemy engine. Please check your configuration. Original error: {e}"
            if is_explicit_path:
                raise RuntimeError(error_msg) from e
            else:
                settings.database_connection_error_message = error_msg
                if verbose:
                    print(f"[EngineFetcher] Error: {error_msg}")

    @staticmethod
    def _build_sql_url(settings: Dynaconf) -> str:
        """Builds a SQL connection URL from a Dynaconf object."""
        try:
            db_type = str(settings.DB_TYPE).upper()
        except AttributeError:
            db_type = 'MYSQL'

        if db_type == 'DATABRICKS':
            try:
                return str(settings.JDBC_URL)
            except AttributeError:
                raise ValueError("Missing JDBC_URL for Databricks connection")

        if db_type == 'SQLITE':
            try:
                return f"sqlite:///{str(settings.DB_DATABASE)}"
            except AttributeError:
                raise ValueError("Missing required DB_DATABASE path for SQLite connection")

        try:
            username = str(settings.GX_USERNAME)
            password = str(settings.GX_PASSWORD)
            host = str(settings.DB_HOST)
            port = str(settings.DB_PORT)
            database = str(settings.DB_DATABASE)
        except AttributeError as e:
            raise ValueError(f"Missing required database credential: {e.name}") from e

        if db_type == 'POSTGRESQL':
            return f"postgresql+psycopg2://{username}:{password}@{host}:{port}/{database}"
        elif db_type == 'MYSQL':
            return f"mysql+pymysql://{username}:{password}@{host}:{port}/{database}"
        else:
            driver_mapping = {'ORACLE': 'oracle+cx_oracle', 'SQLSERVER': 'mssql+pyodbc', 'MSSQL': 'mssql+pyodbc'}
            driver = driver_mapping.get(db_type, db_type.lower())
            return f"{driver}://{username}:{password}@{host}:{port}/{database}"
