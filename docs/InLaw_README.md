# InLaw - Great Expectations Wrapper

The InLaw class is a lightweight wrapper around Great Expectations (GX) that provides a simple, clean interface for creating data validation tests. It follows the "In-Law" pattern: tests run after your main pipeline and loudly complain but never block execution.

## Features

- **Zero GX boilerplate**: Parent class handles all Great Expectations setup
- **Single-file child classes**: Each test lives in its own class for easy AI-generation and review
- **ANSI color console output**: Green for pass, red for fail
- **Automatic test discovery**: Finds and runs all InLaw subclasses
- **Aggregated summary**: Shows total passes/failures at the end

## Installation

Make sure you have the required dependencies installed:

```bash
pip install sqlalchemy pandas great-expectations
```

Or install the full plainerflow package:

```bash
pip install -r requirements.txt
```

## Basic Usage

### 1. Import the InLaw class

```python
from plainerflow import InLaw
```

### 2. Create a test class

```python
class InLawExpectFewerThanThousandRows(InLaw):
    title = "Ensure table has < 1,000 rows"

    @staticmethod
    def run(engine):
        # 1) Compose SQL
        sql = "SELECT COUNT(*) AS n FROM books"
        # 2) Convert to GX dataframe
        gx_df = InLaw.to_gx_dataframe(sql, engine)
        # 3) Run expectation
        result = gx_df.expect_column_values_to_be_between(
            column="n", min_value=0, max_value=999
        )
        # 4) Pass/fail
        if result.success:
            return True
        return f"Row count = {gx_df.iloc[0]['n']}"
```

### 3. Run all tests

```python
import sqlalchemy

# Create your database engine
engine = sqlalchemy.create_engine("your_database_url")

# Run all InLaw tests
results = InLaw.run_all(engine)
```

## Child Class Requirements

Each InLaw child class must implement:

1. **`title`** (class attribute): A descriptive string for the test
2. **`run(engine)`** (static method): The test logic that returns:
   - `True` if the test passes
   - `str` if the test fails (the string becomes the error message)

## Available Helper Methods

### `InLaw.to_gx_dataframe(sql, engine)`

Converts SQL query results to a Great Expectations DataFrame.

```python
sql = "SELECT COUNT(*) as row_count FROM my_table"
gx_df = InLaw.to_gx_dataframe(sql, engine)
```

### `InLaw.ansi_green(text)` / `InLaw.ansi_red(text)`

Add ANSI color codes for console output.

```python
print(InLaw.ansi_green("✅ PASS"))
print(InLaw.ansi_red("❌ FAIL"))
```

### `InLaw.run_all(engine)`

Discovers and runs all InLaw subclasses, returning a summary dictionary.

## Example Output

```bash
===== IN-LAW TESTS =====
▶ Running: Ensure table has < 1,000 rows
✅ PASS
▶ Running: Check authors table has no null names
❌ FAIL: 12 null values found in column "name"
============================================
Summary: 1 passed · 1 failed
```

## Complete Example

```python
import sqlalchemy
from plainerflow import InLaw

# Define your tests
class InLawRowCountCheck(InLaw):
    title = "Verify reasonable row count"
    
    @staticmethod
    def run(engine):
        sql = "SELECT COUNT(*) as total_rows FROM users"
        gx_df = InLaw.to_gx_dataframe(sql, engine)
        
        result = gx_df.expect_column_values_to_be_between(
            column="total_rows", 
            min_value=1, 
            max_value=1000000
        )
        
        if result.success:
            return True
        return f"Row count {gx_df.iloc[0]['total_rows']} is outside expected range"

class InLawNoNullEmails(InLaw):
    title = "Check for null email addresses"
    
    @staticmethod
    def run(engine):
        sql = "SELECT COUNT(*) as null_emails FROM users WHERE email IS NULL"
        gx_df = InLaw.to_gx_dataframe(sql, engine)
        
        result = gx_df.expect_column_values_to_equal(
            column="null_emails", 
            value=0
        )
        
        if result.success:
            return True
        return f"Found {gx_df.iloc[0]['null_emails']} users with null emails"

# Run the tests
if __name__ == "__main__":
    engine = sqlalchemy.create_engine("sqlite:///example.db")
    results = InLaw.run_all(engine)
    
    print(f"\nTest Summary:")
    print(f"Passed: {results['passed']}")
    print(f"Failed: {results['failed']}")
    print(f"Errors: {results['errors']}")
```

## Error Handling

The InLaw framework handles errors gracefully:

- **SQL errors**: Caught and reported as test errors
- **Invalid return types**: Detected and reported as errors
- **Exceptions in test logic**: Caught and displayed with error details

## Great Expectations Integration

InLaw uses Great Expectations under the hood, so you have access to all GX expectations:

- `expect_column_values_to_be_between()`
- `expect_column_values_to_equal()`
- `expect_column_values_to_not_be_null()`
- `expect_column_values_to_match_regex()`
- And many more...

See the [Great Expectations documentation](https://docs.greatexpectations.io/) for a complete list of available expectations.

## Best Practices

1. **Keep tests focused**: Each test should validate one specific aspect
2. **Use descriptive titles**: Make it clear what each test is checking
3. **Provide helpful error messages**: Return meaningful strings when tests fail
4. **Test your tests**: Create tests that you know should pass and fail
5. **Use appropriate SQL**: Write efficient queries that return the data you need to validate

## Troubleshooting

### Import Errors

If you get import errors for `sqlalchemy` or `great_expectations`, make sure they're installed:

```bash
pip install sqlalchemy pandas great-expectations
```

### No Tests Found

If `InLaw.run_all()` reports "No InLaw test classes found", make sure:

1. Your test classes inherit from `InLaw`
2. Your test classes are imported/defined in the same namespace
3. Your test classes implement the required `title` and `run()` method

### Database Connection Issues

Make sure your SQLAlchemy engine is properly configured and can connect to your database before running tests.

## Great Expectation Tests List

Here is the list of Great Expectation expectation functions.
For documentation on how to use these please see <https://greatexpectations.io/expectations/>

### Use these frequently

These expectations tend to do well, when the underlying test is actually implemented in SQL, and then the resulting handful of rows
are subject to these simpler expectations. SQL is far more performant than allowing great expectations to handle millions of rows in a dataframe.

- expect_table_row_count_to_equal
- expect_table_row_count_to_be_between
- expect_column_sum_to_be_between
- expect_column_values_to_be_between
- expect_column_values_to_be_null
- expect_column_values_to_not_be_null
- expect_column_values_to_be_unique

### Use these sometimes

These are often slower, but can solve more complex problems.

- expect_column_values_to_match_regex
- expect_column_max_to_be_between
- expect_column_mean_to_be_between
- expect_column_median_to_be_between
- expect_column_min_to_be_between
- expect_column_to_exist
- expect_column_unique_value_count_to_be_between
- expect_column_values_to_be_decreasing
- expect_column_values_to_be_increasing
- expect_column_values_to_match_regex_list
- expect_table_column_count_to_be_between
- expect_table_column_count_to_equal
- expect_table_row_count_to_equal_other_table

### Use these rarely

These are slow statistical tests that should be used rarely.

- expect_column_distinct_values_to_be_in_set
- expect_column_distinct_values_to_contain_set
- expect_column_distinct_values_to_equal_set
- expect_column_most_common_value_to_be_in_set
- expect_column_pair_cramers_phi_value_to_be_less_than
- expect_column_pair_values_a_to_be_greater_than_b
- expect_column_pair_values_to_be_equal
- expect_column_value_lengths_to_be_between
- expect_column_value_lengths_to_equal
- expect_column_values_to_be_dateutil_parseable
- expect_column_values_to_be_in_set
- expect_column_values_to_be_in_type_list
- expect_column_values_to_be_json_parseable
- expect_column_values_to_be_of_type
- expect_column_values_to_match_like_pattern_list
- expect_column_values_to_match_like_pattern
- expect_column_values_to_match_strftime_format
- expect_column_values_to_not_be_in_set
- expect_column_values_to_not_match_like_pattern_list
- expect_column_values_to_not_match_like_pattern
- expect_column_values_to_not_match_regex_list
- expect_column_values_to_not_match_regex
- expect_compound_columns_to_be_unique
- expect_multicolumn_sum_to_equal
- expect_multicolumn_values_to_be_unique
- expect_query_results_to_match_comparison
- expect_select_column_values_to_be_unique_within_record
- expect_table_columns_to_match_ordered_list
- expect_table_columns_to_match_set
