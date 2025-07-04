"""
Example usage of the InLaw Great Expectations wrapper.

This demonstrates how to create custom validation tests using the InLaw framework.
"""

import sqlalchemy
from plainerflow import InLaw


# Example 1: Row count validation (< 100 rows) - Most common pattern
class InLawTableRowCountCheck(InLaw):
    title = "Ensure test_table has < 100 rows"

    @staticmethod
    def run(engine):
        # Common pattern: SELECT COUNT(*) and check it's under a threshold
        sql = "SELECT COUNT(*) AS row_count FROM test_table"
        count_gx_df = InLaw.sql_to_gx_df(sql=sql, engine=engine)
        
        result = count_gx_df.expect_column_values_to_be_between(
            column="row_count", min_value=0, max_value=99
        )
        
        if result.success:
            return True
        return "Table has too many rows, expected < 100"


# Example 2: Zero rows returned - Second most common pattern
class InLawNoInvalidRecords(InLaw):
    title = "Check for zero invalid records"

    @staticmethod
    def run(engine):
        # Common pattern: Query should return zero rows (no problems found)
        sql = "SELECT COUNT(*) as invalid_count FROM test_table WHERE value < 0"  # Should return 0
        invalid_gx_df = InLaw.sql_to_gx_df(sql=sql, engine=engine)
        
        # Check that count is zero
        result = invalid_gx_df.expect_column_values_to_be_between(column="invalid_count", min_value=0, max_value=0)
        if result.success:
            return True
        return "Found invalid records with negative values"


# Example 3: Exactly one row returned - Third most common pattern  
class InLawSingleConfigRecord(InLaw):
    title = "Verify exactly one configuration record exists"

    @staticmethod
    def run(engine):
        # Common pattern: Query should return exactly one row
        sql = "SELECT COUNT(*) as config_count FROM test_table WHERE name = 'Alice'"
        config_gx_df = InLaw.sql_to_gx_df(sql=sql, engine=engine)
        
        result = config_gx_df.expect_column_values_to_be_between(
            column="config_count", min_value=1, max_value=1
        )
        
        if result.success:
            return True
        return "Expected exactly 1 Alice record, but count was different"


# Example 4: Single row with numeric value in range - Fourth most common pattern
class InLawNumericValueInRange(InLaw):
    title = "Verify numeric column value is between 7 and 10"

    @staticmethod
    def run(engine):
        # Common pattern: Single row query with numeric value validation
        sql = "SELECT value as this_particular_col FROM test_table WHERE name = 'Bob'"
        numeric_gx_df = InLaw.sql_to_gx_df(sql=sql, engine=engine)
        
        result = numeric_gx_df.expect_column_values_to_be_between(
            column="this_particular_col", min_value=7, max_value=10
        )
        
        if result.success:
            return True
        return "Value is not between 7 and 10"


# Example 5: Single row with non-null VARCHAR - Fifth most common pattern
class InLawVarcharNotBlankOrNull(InLaw):
    title = "Verify VARCHAR column is not blank or NULL"

    @staticmethod
    def run(engine):
        # Common pattern: Single row query with VARCHAR validation
        sql = "SELECT name as that_other_col FROM test_table WHERE name = 'Alice'"
        varchar_gx_df = InLaw.sql_to_gx_df(sql=sql, engine=engine)
        
        # Check column is not null
        result_not_null = varchar_gx_df.expect_column_values_to_not_be_null(
            column="that_other_col"
        )
        
        if not result_not_null.success:
            return "Column 'that_other_col' contains NULL values"
        
        return True


# Example 6: Always failing test for demonstration
class InLawAlwaysFailsTest(InLaw):
    title = "Test that always fails (for demo)"

    @staticmethod
    def run(engine):
        # This test is designed to always fail
        sql = "SELECT 100 as large_number"
        fail_gx_df = InLaw.sql_to_gx_df(sql=sql, engine=engine)
        
        result = fail_gx_df.expect_column_values_to_be_between(
            column="large_number", min_value=0, max_value=50
        )
        
        if result.success:
            return True
        return "Value is not between 0 and 50"


def main():
    """
    Example of how to use InLaw with a database connection.
    
    Note: This example uses SQLite for simplicity, but InLaw works with any
    SQLAlchemy-supported database.
    """
    
    # Create a simple in-memory SQLite database for demonstration
    engine = sqlalchemy.create_engine("sqlite:///:memory:")
    
    # Create a simple test table
    with engine.connect() as conn:
        conn.execute(sqlalchemy.text("""
            CREATE TABLE test_table (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                value INTEGER
            )
        """))
        
        # Insert some test data
        conn.execute(sqlalchemy.text("""
            INSERT INTO test_table (name, value) VALUES 
            ('Alice', 100),
            ('Bob', 8),
            ('Charlie', 300)
        """))
        conn.commit()
    
    print("Running InLaw validation tests...")
    print()
    
    # Run all InLaw tests
    results = InLaw.run_all(engine=engine)
    
    print()
    print("Test Results Summary:")
    print(f"Total tests: {results['total']}")
    print(f"Passed: {results['passed']}")
    print(f"Failed: {results['failed']}")
    print(f"Errors: {results['errors']}")
    
    return results


if __name__ == "__main__":
    main()
