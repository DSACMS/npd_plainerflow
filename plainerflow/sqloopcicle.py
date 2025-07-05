"""
SQLoopcicle - A single-purpose utility for looping over SQL statements.

Provides a simple loop-and-run mechanism for executing SQL statements from a dictionary
with support for both actual execution and dry-run mode with clear console output.
Built for clarity, testability, and minimal abstraction.
"""

from typing import Dict, Union, Mapping
from sqlalchemy import text
from sqlalchemy.engine import Engine
import pandas as pd
import time
import datetime as dt
import human_readable


class SQLoopcicle:
    """
    A single-purpose utility for looping over a mapping of SQL statements 
    and running them using a SQLAlchemy engine.
    
    Supports both actual execution and dry-run mode with clear console output.
    Minimal, functional abstraction aligned with PlainerFlow's "no-fluff" philosophy.
    """
    
    @staticmethod
    def get_sql_type_icon(sql_string: str) -> str:
        """
        Detect the type of SQL statement and return the appropriate icon.
        
        Args:
            sql_string: The SQL statement to analyze
            
        Returns:
            str: Icon representing the SQL statement type:
                🔻 for DROP statements
                📥 for INSERT statements
                Green ▣ for CREATE statements
                Green ⇲ for CREATE TABLE AS SELECT (CTAS) statements
                🔍 for SELECT statements
        """
        # ANSI color codes
        GREEN = "\033[32m"
        RESET = "\033[0m"
        
        # Normalize the SQL string for analysis
        sql_upper = sql_string.strip().upper()
        
        # Check for DROP statements
        if sql_upper.startswith('DROP'):
            return '🔻'
        
        # Check for INSERT statements
        if sql_upper.startswith('INSERT'):
            return '📥'
        
        # Check for CREATE TABLE AS SELECT (CTAS) - must come before general CREATE check
        if sql_upper.startswith('CREATE TABLE') and ' AS SELECT' in sql_upper:
            return f'{GREEN}⇲{RESET}'
        
        # Check for general CREATE statements
        if sql_upper.startswith('CREATE'):
            return f'{GREEN}▣{RESET}'
        
        # Check for SELECT statements
        if sql_upper.startswith('SELECT'):
            return '🔍'
        
        # Default icon for other statement types
        return '▶'
    
    @staticmethod
    def run_sql_loop(
        sql_dict: Mapping[str, str],
        engine: Engine,
        *,
        is_just_print: bool = False,
        is_display_select: bool = True,
        select_display_rows: int = 50
    ) -> None:
        """
        Execute SQL statements from a dictionary in order.
        
        Args:
            sql_dict: Mapping of keys to SQL statements. Executed in dictionary order.
            engine: A live SQLAlchemy engine for database interaction.
            is_just_print: If True, SQL statements are printed instead of executed.
                          Keyword-only parameter.
            is_display_select: If True, SELECT query results are displayed.
                              Keyword-only parameter. Defaults to True.
            select_display_rows: Number of rows to display from SELECT queries.
                               Keyword-only parameter. Defaults to 50.
        
        Raises:
            No exceptions are raised. SQL errors are caught and handled gracefully,
            with error messages printed and execution terminated on first error.
        
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
            print("⏩ =====  DRY-RUN MODE – NO SQL WILL BE EXECUTED =====")
        else:
            print("⏩ =====  EXECUTING SQL LOOP =====")
        
        # Single loop: print and execute each SQL statement
        if not is_just_print and sql_dict:
            try:
                with engine.connect() as conn:
                    for key, sql_string in sql_dict.items():
                        # Print the SQL statement with appropriate icon
                        icon = SQLoopcicle.get_sql_type_icon(sql_string)
                        print(f"{icon} {key}:\n{sql_string}\n")
                        
                        sql_upper = sql_string.strip().upper()
                        
                        # Handle SELECT queries differently if display is enabled
                        if (sql_upper.startswith('SELECT') and 
                            is_display_select and 
                            not (sql_upper.startswith('CREATE TABLE') and ' AS SELECT' in sql_upper)):
                            
                            # Use pandas to read and display SELECT results
                            try:
                                start_time = time.time()
                                select_data = pd.read_sql_query(sql_string, conn, params=None)
                                end_time = time.time()
                                
                                # Calculate and display execution time
                                execution_time = end_time - start_time
                                time_delta = dt.timedelta(seconds=execution_time)
                                readable_time = human_readable.precise_delta(time_delta, minimum_unit="seconds")
                                print(f"⏱️  Query executed in: {readable_time}")
                                
                                if len(select_data) > 0:
                                    # Limit rows displayed
                                    display_data = select_data.head(select_display_rows)
                                    print(f"📊 Results for {key} (showing {len(display_data)} of {len(select_data)} rows):")
                                    print(display_data.to_string(index=False))
                                    print()  # Add blank line for readability
                                else:
                                    print(f"📊 Results for {key}: No rows returned")
                                    print()
                            except Exception as e:
                                print(f"❌ Error executing SELECT query {key}: {e}")
                                print("🛑 SQL loop terminated due to error")
                                return
                        else:
                            # Execute non-SELECT queries or when display is disabled
                            try:
                                start_time = time.time()
                                with engine.begin() as trans_conn:
                                    trans_conn.execute(text(sql_string))
                                end_time = time.time()
                                
                                # Calculate and display execution time
                                execution_time = end_time - start_time
                                time_delta = dt.timedelta(seconds=execution_time)
                                readable_time = human_readable.precise_delta(time_delta, minimum_unit="seconds")
                                print(f"⏱️  Query executed in: {readable_time}")
                                print()  # Add blank line for readability
                            except Exception as e:
                                line = '-' * 80
                                print(f"❌ Error executing SQL query {key}:\nError Start {line}v\n\n{e}\n\n^{line}----------- Error End")
                                print("🛑 SQL loop terminated due to error")
                                return
            except Exception as e:
                print(f"❌ Database connection or general error:\nError---\n\n{e}\n---")
                print("🛑 SQL loop terminated due to error")
                return
        else:
            # Dry-run mode: just print the SQL statements
            for key, sql_string in sql_dict.items():
                # Print the SQL statement with appropriate icon
                icon = SQLoopcicle.get_sql_type_icon(sql_string)
                print(f"{icon} {key}:\n{sql_string}\n")
        
        # Print end message
        print("⏪ ===== SQL LOOP COMPLETE =====")
        if is_just_print:
            print("🟡 ===== I AM NOT RUNNING SQL =====")
