"""
InLaw - A lightweight wrapper around Great Expectations (GX)

The "In-Law" pattern: Tests run after your main pipeline—loudly complain but never block.
Single-file child classes with zero GX boilerplate.
"""

import sys
import os
import importlib.util
import warnings
from abc import ABC, abstractmethod
from typing import Union, List, Dict, Any, Optional
import sqlalchemy
import pandas as pd

try:
    import great_expectations as gx
except ImportError:
    raise ImportError(
        "Great Expectations is required for InLaw. Install with: pip install great-expectations"
    )

# Suppress Great Expectations checkpoint warnings since InLaw replaces checkpoints
warnings.filterwarnings(
    "ignore",
    message=r".*result_format.*configured at the Validator-level will not be persisted.*",
    category=UserWarning,
    module="great_expectations.expectations.expectation"
)


class _SuppressGXWarnings:
    """Context manager to suppress Great Expectations checkpoint warnings."""
    
    def __enter__(self):
        warnings.filterwarnings(
            "ignore",
            message=r".*result_format.*configured at the Validator-level will not be persisted.*",
            category=UserWarning
        )
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        # Reset warnings to default behavior for this specific warning
        warnings.filterwarnings(
            "default",
            message=r".*result_format.*configured at the Validator-level will not be persisted.*",
            category=UserWarning
        )


class InLaw(ABC):
    """
    Abstract base class for Great Expectations validation tests.
    
    Child classes must implement:
    - title: str (class attribute)
    - run(engine) -> bool | str (static method)
    """
    
    title: str = "Unnamed Test"
    
    @staticmethod
    @abstractmethod
    def run(engine) -> Union[bool, str]:
        """
        Run the validation test.
        
        Args:
            engine: SQLAlchemy engine for database connection
            
        Returns:
            True if test passes
            str if test fails (error message)
        """
        pass
    
        
    @staticmethod
    def sql_to_gx_df(*, sql: str, engine):
        """
        Convert SQL query result to Great Expectations DataFrame.
        
        Args:
            sql: SQL query string
            engine: SQLAlchemy engine
            
        Returns:
            Great Expectations DataFrame
        """
        try:
            # Execute SQL and get pandas DataFrame
            with engine.connect() as conn:
                pandas_df = pd.read_sql_query(sqlalchemy.text(sql), conn)
            
            # Suppress only the specific result_format warning during GX operations
            with warnings.catch_warnings():
                warnings.filterwarnings(
                    "ignore",
                    message=r".*result_format.*configured at the Validator-level will not be persisted.*",
                    category=UserWarning
                )
                
                # Convert to Great Expectations DataFrame using the correct API
                context = gx.get_context()
                datasource = context.sources.add_pandas("pandas_datasource")
                data_asset = datasource.add_dataframe_asset("dataframe_asset")
                batch_request = data_asset.build_batch_request(dataframe=pandas_df)
                gx_df = context.get_validator(batch_request=batch_request)
            
            return gx_df
            
        except Exception as e:
            raise RuntimeError(f"Failed to execute SQL and create GX DataFrame: {e}")
    
    @staticmethod
    def to_gx_dataframe(sql: str, engine):
        """
        Legacy method name - use sql_to_gx_df instead.
        Convert SQL query result to Great Expectations DataFrame.
        
        Args:
            sql: SQL query string
            engine: SQLAlchemy engine
            
        Returns:
            Great Expectations DataFrame
        """
        return InLaw.sql_to_gx_df(sql=sql, engine=engine)
    
    @staticmethod
    def ansi_green(text: str) -> str:
        """Return text with ANSI green color codes."""
        return f"\033[92m{text}\033[0m"
    
    @staticmethod
    def ansi_red(text: str) -> str:
        """Return text with ANSI red color codes."""
        return f"\033[91m{text}\033[0m"

    @staticmethod
    def _import_file(*, file_path: str) -> None:
        """
        Import a Python file to discover InLaw subclasses.
        
        Args:
            file_path: Path to the Python file to import
        """
        try:
            # Get absolute path
            abs_path = os.path.abspath(file_path)
            
            # Create module name from file path
            module_name = os.path.splitext(os.path.basename(abs_path))[0]
            
            # Load the module
            spec = importlib.util.spec_from_file_location(module_name, abs_path)
            if spec and spec.loader:
                module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(module)
                
        except Exception as e:
            print(f"Warning: Failed to import {file_path}: {e}")
    
    @staticmethod
    def _import_directory(*, directory_path: str) -> None:
        """
        Import all Python files in a directory to discover InLaw subclasses.
        
        Args:
            directory_path: Path to the directory containing Python files
        """
        try:
            if not os.path.isdir(directory_path):
                print(f"Warning: Directory {directory_path} does not exist")
                return
                
            for filename in os.listdir(directory_path):
                if filename.endswith('.py') and not filename.startswith('__'):
                    file_path = os.path.join(directory_path, filename)
                    InLaw._import_file(file_path=file_path)
                    
        except Exception as e:
            print(f"Warning: Failed to import from directory {directory_path}: {e}")

    @staticmethod
    def run_all(*, engine, inlaw_files: Optional[List[str]] = None, inlaw_dir: Optional[str] = None) -> Dict[str, Any]:
        """
        Discover and run all InLaw subclasses.
        
        Args:
            engine: SQLAlchemy engine for database connection
            inlaw_files: Optional list of relative file paths to import for InLaw tests
            inlaw_dir: Optional directory path to scan for InLaw test files
            
        Returns:
            Dictionary with test results summary
        """
        print("===== IN-LAW TESTS =====")
        
        # Import additional files if specified
        if inlaw_files:
            for file_path in inlaw_files:
                InLaw._import_file(file_path=file_path)
        
        # Import from directory if specified
        if inlaw_dir:
            InLaw._import_directory(directory_path=inlaw_dir)
        
        # Discover all subclasses (including newly imported ones)
        subclasses = InLaw.__subclasses__()
        
        if not subclasses:
            print("No InLaw test classes found.")
            return {"passed": 0, "failed": 0, "errors": 0, "total": 0}
        
        passed = 0
        failed = 0
        errors = 0
        results = []
        
        for test_class in subclasses:
            test_title = getattr(test_class, 'title', test_class.__name__)
            print(f"▶ Running: {test_title}")
            
            try:
                # Suppress only the specific result_format warning during test execution
                with warnings.catch_warnings():
                    warnings.filterwarnings(
                        "ignore",
                        message=r".*result_format.*configured at the Validator-level will not be persisted.*",
                        category=UserWarning
                    )
                    result = test_class.run(engine)
                
                if result is True:
                    print(InLaw.ansi_green("✅ PASS"))
                    passed += 1
                    results.append({"test": test_title, "status": "PASS", "message": None})
                elif isinstance(result, str):
                    print(InLaw.ansi_red(f"❌ FAIL: {result}"))
                    failed += 1
                    results.append({"test": test_title, "status": "FAIL", "message": result})
                else:
                    error_msg = f"Invalid return type from test: {type(result)}. Expected bool or str."
                    print(InLaw.ansi_red(f"💥 ERROR: {error_msg}"))
                    errors += 1
                    results.append({"test": test_title, "status": "ERROR", "message": error_msg})
                    
            except Exception as e:
                error_msg = f"Exception in test: {str(e)}"
                print(InLaw.ansi_red(f"💥 ERROR: {error_msg}"))
                errors += 1
                results.append({"test": test_title, "status": "ERROR", "message": error_msg})
        
        # Print summary
        print("=" * 44)
        summary_parts = []
        if passed > 0:
            summary_parts.append(f"{passed} passed")
        if failed > 0:
            summary_parts.append(f"{failed} failed")
        if errors > 0:
            summary_parts.append(f"{errors} errors")
            
        summary = "Summary: " + " · ".join(summary_parts) if summary_parts else "Summary: No tests run"
        print(summary)
        
        return {
            "passed": passed,
            "failed": failed, 
            "errors": errors,
            "total": len(subclasses),
            "results": results
        }
    
    @staticmethod
    def run_all_legacy(engine, inlaw_files: Optional[List[str]] = None, inlaw_dir: Optional[str] = None) -> Dict[str, Any]:
        """
        Legacy version of run_all that accepts engine as positional argument.
        Use run_all with named parameters instead.
        """
        return InLaw.run_all(engine=engine, inlaw_files=inlaw_files, inlaw_dir=inlaw_dir)


# Example child class for demonstration
class InLawExampleTest(InLaw):
    """Example InLaw test class - can be removed in production."""
    
    title = "Example test that always passes"
    
    @staticmethod
    def run(engine):
        """Example test that demonstrates the pattern."""
        # Simple test that always passes
        sql = "SELECT 1 as test_value"
        gx_df = InLaw.sql_to_gx_df(sql=sql, engine=engine)
        
        # Suppress only the specific result_format warning during expectation calls
        with warnings.catch_warnings():
            warnings.filterwarnings(
                "ignore",
                message=r".*result_format.*configured at the Validator-level will not be persisted.*",
                category=UserWarning
            )
            
            result = gx_df.expect_column_values_to_be_between(
                column="test_value", 
                min_value=0, 
                max_value=2
            )
        
        if result.success:
            return True
        return f"Test value was not between 0 and 2"
