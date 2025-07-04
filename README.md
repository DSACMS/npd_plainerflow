# plainerflow

A Python package for plain flow operations with SQLAlchemy integration.

## Installation

### From PyPI (recommended)

```bash
pip install plainerflow
```

### Manual Installation

If pip is not available in your environment, you can use the package by adding it to your Python path:

```python
import sys
sys.path.insert(0, "/path/to/the/right/plainerflow/subdirectory")
import plainerflow
```

## Using PlainerFlow

PlainerFlow provides a set of components designed to work together for data transformation pipelines. Here's a complete example showing how to import CSV data, transform it using SQL, and validate the results.

### Complete Pipeline Example

This example demonstrates importing customer and order data, creating derived tables, and validating the transformation results.

```python
import plainerflow
from plainerflow import CredentialFinder, DBTable, FrostDict, SQLoopcicle, InLaw
import pandas as pd
from pathlib import Path

# Step 1: Get database connection (automatically detects environment)
engine = CredentialFinder.detect_config(verbose=True)

# Step 2: Define target tables using DBTable before they exist
customers_table = DBTable(database='analytics', table='customers')
orders_table = DBTable(database='analytics', table='orders')
customer_summary_table = DBTable(database='analytics', table='customer_summary')

print(f"Will create tables: {customers_table}, {orders_table}, {customer_summary_table}")

# Step 3: Load CSV data into pandas DataFrames
customers_df = pd.read_csv('readme_example_data/customers.csv')
orders_df = pd.read_csv('readme_example_data/orders.csv')

# Step 4: Import data to SQLite
customers_df.to_sql('customers', engine, if_exists='replace', index=False)
orders_df.to_sql('orders', engine, if_exists='replace', index=False)

print("CSV data imported to SQLite")

# Step 5: Define transformation SQL using FrostDict
transformation_sql = FrostDict({
    'create_customer_summary': f"""
        CREATE TABLE IF NOT EXISTS {customer_summary_table} AS
        SELECT 
            c.customer_id,
            c.first_name || ' ' || c.last_name as full_name,
            c.email,
            c.status as customer_status,
            COUNT(o.order_id) as total_orders,
            COALESCE(SUM(o.quantity * o.unit_price), 0) as total_spent,
            MAX(o.order_date) as last_order_date
        FROM {customers_table} c
        LEFT JOIN {orders_table} o ON c.customer_id = o.customer_id
        GROUP BY c.customer_id, c.first_name, c.last_name, c.email, c.status
    """,
    
    'create_order_metrics': f"""
        CREATE TABLE IF NOT EXISTS order_metrics AS
        SELECT 
            order_status,
            COUNT(*) as order_count,
            AVG(quantity * unit_price) as avg_order_value,
            SUM(quantity * unit_price) as total_value
        FROM {orders_table}
        GROUP BY order_status
    """
})

# Step 6: Execute transformations using SQLoopcicle
print("\nRunning data transformations...")
SQLoopcicle.run_sql_loop(transformation_sql, engine)

# Step 7: Create validation tests using InLaw
class ValidateCustomerSummaryRowCount(InLaw):
    title = "Customer summary should have same number of rows as customers"
    
    @staticmethod
    def run(engine):
        sql = f"""
        SELECT 
            (SELECT COUNT(*) FROM {customers_table}) as customer_count,
            (SELECT COUNT(*) FROM {customer_summary_table}) as summary_count
        """
        gdf = InLaw.to_gx_dataframe(sql, engine)
        
        customer_count = gdf.iloc[0]['customer_count']
        summary_count = gdf.iloc[0]['summary_count']
        
        if customer_count == summary_count:
            return True
        return f"Row count mismatch: {customer_count} customers vs {summary_count} summary rows"

class ValidateNoNullCustomerNames(InLaw):
    title = "Customer summary should have no null names"
    
    @staticmethod
    def run(engine):
        sql = f"SELECT COUNT(*) as null_count FROM {customer_summary_table} WHERE full_name IS NULL"
        gdf = InLaw.to_gx_dataframe(sql, engine)
        
        result = gdf.expect_column_values_to_equal(column="null_count", value=0)
        
        if result.success:
            return True
        return f"Found {gdf.iloc[0]['null_count']} null names in customer summary"

class ValidateTotalSpentIsPositive(InLaw):
    title = "Active customers with orders should have positive total_spent"
    
    @staticmethod
    def run(engine):
        sql = f"""
        SELECT COUNT(*) as invalid_count 
        FROM {customer_summary_table} 
        WHERE total_orders > 0 AND total_spent <= 0
        """
        gdf = InLaw.to_gx_dataframe(sql, engine)
        
        result = gdf.expect_column_values_to_equal(column="invalid_count", value=0)
        
        if result.success:
            return True
        return f"Found {gdf.iloc[0]['invalid_count']} customers with orders but non-positive spending"

# Step 8: Run validation tests
print("\nRunning data validation tests...")
test_results = InLaw.run_all(engine)

# Step 9: Display sample results
print("\nSample Results:")
with engine.connect() as conn:
    sample_data = pd.read_sql_query(f"SELECT * FROM {customer_summary_table} LIMIT 3", conn)
    print(sample_data.to_string(index=False))

print(f"\nPipeline completed successfully!")
print(f"   - Imported {len(customers_df)} customers and {len(orders_df)} orders")
print(f"   - Created {customer_summary_table} and order_metrics tables")
print(f"   - Validation results: {test_results['passed']} passed, {test_results['failed']} failed")
```

### Key Components Explained

#### 1. CredentialFinder - Automatic Database Connection
```python
# Automatically detects your environment and provides a database connection
engine = CredentialFinder.detect_config(verbose=True)
# Supports: Spark/Databricks, Google Colab, .env files, SQLite fallback
```

#### 2. DBTable - Database Table References
```python
# Define table references before tables exist
customers_table = DBTable(database='analytics', table='customers')
orders_table = DBTable(database='analytics', table='orders')

# Create child tables with suffixes
backup_table = customers_table.make_child('backup')  # analytics.customers_backup

# Use in SQL queries via f-strings
sql = f"SELECT * FROM {customers_table} WHERE status = 'active'"
```

#### 3. FrostDict - Immutable SQL Configuration
```python
# Create frozen dictionary for SQL templates
sql_queries = FrostDict({
    'create_table': f"CREATE TABLE {table_name} AS SELECT ...",
    'update_data': f"UPDATE {table_name} SET ..."
})

# Keys cannot be reassigned once set
sql_queries['new_query'] = "SELECT 1"  # Works - new key
sql_queries['create_table'] = "SELECT 2"  # Raises FrozenKeyError
```

#### 4. SQLoopcicle - SQL Execution Loop
```python
# Execute SQL statements in order
SQLoopcicle.run_sql_loop(sql_queries, engine)

# Dry-run mode to preview without execution
SQLoopcicle.run_sql_loop(sql_queries, engine, is_just_print=True)
```

#### 5. InLaw - Data Validation Framework
```python
# Create validation test classes
class MyDataTest(InLaw):
    title = "Descriptive test name"
    
    @staticmethod
    def run(engine):
        sql = "SELECT COUNT(*) as row_count FROM my_table"
        gdf = InLaw.to_gx_dataframe(sql, engine)
        
        result = gdf.expect_column_values_to_be_between(
            column="row_count", min_value=1, max_value=1000
        )
        
        return True if result.success else f"Row count out of range: {gdf.iloc[0]['row_count']}"

# Run all validation tests
InLaw.run_all(engine)
```

### Running the Example

1. **Install PlainerFlow:**
   ```bash
   pip install plainerflow pandas great-expectations
   ```

2. **Run the example script:**
   ```python
   # Save the complete example above as 'pipeline_example.py'
   python pipeline_example.py
   ```

3. **Expected output:**
   ```
   [CredentialFinder] Falling back to local SQLite database: ~/plainerflow_fallback.db
   Will create tables: analytics.customers, analytics.orders, analytics.customer_summary
   CSV data imported to SQLite
   
   Running data transformations...
   ===== EXECUTING SQL LOOP =====
   create_customer_summary: CREATE TABLE IF NOT EXISTS analytics.customer_summary AS...
   create_order_metrics: CREATE TABLE IF NOT EXISTS order_metrics AS...
   ===== SQL LOOP COMPLETE =====
   
   Running data validation tests...
   ===== IN-LAW TESTS =====
   Running: Customer summary should have same number of rows as customers
   PASS
   Running: Customer summary should have no null names
   PASS
   Running: Active customers with orders should have positive total_spent
   PASS
   ============================================
   Summary: 3 passed
   
   Sample Results:
   customer_id      full_name              email  customer_status  total_orders  total_spent last_order_date
             1       John Doe   john.doe@email.com           active             2        85.48      2024-01-25
             2     Jane Smith jane.smith@email.com           active             2        91.98      2024-02-20
             3    Bob Johnson  bob.johnson@email.com         inactive             1         0.00      2024-02-02
   
   Pipeline completed successfully!
      - Imported 5 customers and 7 orders
      - Created analytics.customer_summary and order_metrics tables
      - Validation results: 3 passed, 0 failed
   ```

This example demonstrates PlainerFlow's philosophy: simple, composable components that work together to create robust data pipelines with built-in validation.

## Basic Usage

For simpler use cases, you can use individual components:

```python
import plainerflow

# Just get a database connection
engine = plainerflow.CredentialFinder.detect_config()

# Define a table reference
my_table = plainerflow.DBTable(database='mydb', table='users')
print(f"Table reference: {my_table}")  # Output: mydb.users

# Create frozen configuration
config = plainerflow.FrostDict({'query': f'SELECT * FROM {my_table}'})

# Execute SQL
plainerflow.SQLoopcicle.run_sql_loop(config, engine)
```

## Dependencies

- SQLAlchemy >= 1.4.0

## Development

### Setting up development environment

1. Clone the repository:
```bash
git clone https://github.com/ftrotter/plainerflow.git
cd plainerflow
```

2. Create a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install development dependencies:
```bash
pip install -e ".[dev]"
```

### Building the package

```bash
python -m build
```

### Uploading to PyPI

```bash
python -m twine upload dist/*
```

## License

This project is licensed under the CC0 1.0 Universal License - see the [LICENSE](LICENSE) file for details.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.
