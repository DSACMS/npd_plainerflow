"""
InLaw - A lightweight wrapper around Great Expectations (GX)

The "In-Law" pattern: Tests run after your main pipeline—loudly complain but never block.
Single-file child classes with zero GX boilerplate.
"""

import sys
from abc import ABC, abstractmethod
from typing import Union, List, Dict, Any
import sqlalchemy
import pandas as pd

try:
    import great_expectations as gx
except ImportError:
    raise ImportError(
        "Great Expectations is required for InLaw. Install with: pip install great-expectations"
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
    def to_gx_dataframe(sql: str, engine):
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
            
            # Convert to Great Expectations DataFrame
            gdf = gx.from_pandas(pandas_df)
            return gdf
            
        except Exception as e:
            raise RuntimeError(f"Failed to execute SQL and create GX DataFrame: {e}")
    
    @staticmethod
    def ansi_green(text: str) -> str:
        """Return text with ANSI green color codes."""
        return f"\033[92m{text}\033[0m"
    
    @staticmethod
    def ansi_red(text: str) -> str:
        """Return text with ANSI red color codes."""
        return f"\033[91m{text}\033[0m"
    
    @staticmethod
    def run_all(engine) -> Dict[str, Any]:
        """
        Discover and run all InLaw subclasses.
        
        Args:
            engine: SQLAlchemy engine for database connection
            
        Returns:
            Dictionary with test results summary
        """
        print("===== IN-LAW TESTS =====")
        
        # Discover all subclasses
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


# Example child class for demonstration
class InLawExampleTest(InLaw):
    """Example InLaw test class - can be removed in production."""
    
    title = "Example test that always passes"
    
    @staticmethod
    def run(engine):
        """Example test that demonstrates the pattern."""
        # Simple test that always passes
        sql = "SELECT 1 as test_value"
        gdf = InLaw.to_gx_dataframe(sql, engine)
        
        result = gdf.expect_column_values_to_be_between(
            column="test_value", 
            min_value=0, 
            max_value=2
        )
        
        if result.success:
            return True
        return f"Test value was not between 0 and 2"
