"""
SQLoopcicle - A single-purpose utility for looping over SQL statements.

Provides a simple loop-and-run mechanism for executing SQL statements from a dictionary
with support for both actual execution and dry-run mode with clear console output.
Built for clarity, testability, and minimal abstraction.
"""

from typing import Dict, Union, Mapping
from sqlalchemy import text
from sqlalchemy.engine import Engine


class SQLoopcicle:
    """
    A single-purpose utility for looping over a mapping of SQL statements 
    and running them using a SQLAlchemy engine.
    
    Supports both actual execution and dry-run mode with clear console output.
    Minimal, functional abstraction aligned with PlainerFlow's "no-fluff" philosophy.
    """
    
    @staticmethod
    def run_sql_loop(
        sql_dict: Mapping[str, str],
        engine: Engine,
        *,
        is_just_print: bool = False
    ) -> None:
        """
        Execute SQL statements from a dictionary in order.
        
        Args:
            sql_dict: Mapping of keys to SQL statements. Executed in dictionary order.
            engine: A live SQLAlchemy engine for database interaction.
            is_just_print: If True, SQL statements are printed instead of executed.
                          Keyword-only parameter.
        
        Raises:
            Any SQLAlchemy or database exceptions are propagated to the caller.
            No error handling is performed within this method.
        
        Example:
            >>> from sqlalchemy import create_engine
            >>> engine = create_engine("sqlite:///:memory:")
            >>> sql_queries = {
            ...     "create_table": "CREATE TABLE users (id INTEGER, name TEXT)",
            ...     "insert_data": "INSERT INTO users VALUES (1, 'Alice')"
            ... }
            >>> SQLoopcicle.run_sql_loop(sql_queries, engine)
            ===== EXECUTING SQL LOOP =====
            ▶ create_table: CREATE TABLE users (id INTEGER, name TEXT)
            ▶ insert_data: INSERT INTO users VALUES (1, 'Alice')
            ===== SQL LOOP COMPLETE =====
        """
        # Print startup message
        if is_just_print:
            print("===== DRY-RUN MODE – NO SQL WILL BE EXECUTED =====")
        else:
            print("===== EXECUTING SQL LOOP =====")
        
        # Loop through SQL dictionary
        for key, sql_string in sql_dict.items():
            # Print the SQL statement
            print(f"▶ {key}: {sql_string}")
        
        # Execute all SQL if not in dry-run mode
        if not is_just_print and sql_dict:
            with engine.begin() as conn:
                for key, sql_string in sql_dict.items():
                    conn.execute(text(sql_string))
        
        # Print end message
        if is_just_print:
            print("===== I AM NOT RUNNING SQL =====")
        else:
            print("===== SQL LOOP COMPLETE =====")
