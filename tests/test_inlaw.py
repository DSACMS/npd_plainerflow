"""
Tests for the InLaw Great Expectations wrapper.
"""

import pytest
import sqlalchemy
from plainerflow import InLaw


class TestInLawBasicPass(InLaw):
    """Test class that should always pass."""
    title = "Basic passing test"
    
    @staticmethod
    def run(engine):
        sql = "SELECT 1 as test_value"
        gdf = InLaw.to_gx_dataframe(sql, engine)
        result = gdf.expect_column_values_to_be_between(column="test_value", min_value=0, max_value=2)
        return True if result.success else "Test value was not between 0 and 2"


class TestInLawBasicFail(InLaw):
    """Test class that should always fail."""
    title = "Basic failing test"
    
    @staticmethod
    def run(engine):
        sql = "SELECT 1 as test_value"
        gdf = InLaw.to_gx_dataframe(sql, engine)
        result = gdf.expect_column_values_to_be_between(column="test_value", min_value=5, max_value=10)
        return True if result.success else "Test value was not between 5 and 10"


class TestInLawWithError(InLaw):
    """Test class that should raise an error."""
    title = "Test with error"
    
    @staticmethod
    def run(engine):
        # This will cause an error due to invalid SQL
        sql = "SELECT FROM invalid_syntax"
        gdf = InLaw.to_gx_dataframe(sql, engine)
        return True


def test_inlaw_to_gx_dataframe():
    """Test the to_gx_dataframe static method."""
    engine = sqlalchemy.create_engine("sqlite:///:memory:")
    
    sql = "SELECT 1 as test_col, 'hello' as text_col"
    validator = InLaw.to_gx_dataframe(sql, engine)
    
    # Verify we got a Great Expectations validator
    assert hasattr(validator, 'expect_column_values_to_be_between')
    
    # Test that we can run an expectation
    result = validator.expect_column_values_to_be_between(column="test_col", min_value=0, max_value=2)
    assert result.success


def test_inlaw_ansi_colors():
    """Test ANSI color helper methods."""
    green_text = InLaw.ansi_green("PASS")
    red_text = InLaw.ansi_red("FAIL")
    
    assert "\033[92m" in green_text  # Green color code
    assert "\033[91m" in red_text    # Red color code
    assert "\033[0m" in green_text   # Reset code
    assert "\033[0m" in red_text     # Reset code


def test_inlaw_run_all():
    """Test the run_all method with multiple test classes."""
    engine = sqlalchemy.create_engine("sqlite:///:memory:")
    
    # Run all InLaw tests (includes our test classes defined above)
    results = InLaw.run_all(engine)
    
    # Verify results structure
    assert isinstance(results, dict)
    assert 'passed' in results
    assert 'failed' in results
    assert 'errors' in results
    assert 'total' in results
    assert 'results' in results
    
    # We should have at least our test classes
    assert results['total'] >= 3
    
    # Check that we have some passes, fails, and errors
    assert results['passed'] >= 1  # TestInLawBasicPass should pass
    assert results['failed'] >= 1  # TestInLawBasicFail should fail
    assert results['errors'] >= 1  # TestInLawWithError should error


def test_inlaw_invalid_return_type():
    """Test handling of invalid return types from test methods."""
    
    class TestInLawInvalidReturn(InLaw):
        title = "Test with invalid return"
        
        @staticmethod
        def run(engine):
            return 42  # Invalid return type (should be bool or str)
    
    engine = sqlalchemy.create_engine("sqlite:///:memory:")
    results = InLaw.run_all(engine)
    
    # Should have at least one error due to invalid return type
    assert results['errors'] >= 1


def test_inlaw_abstract_class():
    """Test that InLaw is properly abstract."""
    with pytest.raises(TypeError):
        # Should not be able to instantiate abstract class directly
        InLaw()


if __name__ == "__main__":
    # Run a simple test
    engine = sqlalchemy.create_engine("sqlite:///:memory:")
    print("Testing InLaw implementation...")
    
    # Test basic functionality
    test_inlaw_to_gx_dataframe()
    test_inlaw_ansi_colors()
    print("✅ Basic tests passed")
    
    # Test run_all functionality
    print("\nRunning InLaw.run_all() test:")
    results = InLaw.run_all(engine)
    print(f"Results: {results}")
    
    print("\n✅ All tests completed successfully!")
