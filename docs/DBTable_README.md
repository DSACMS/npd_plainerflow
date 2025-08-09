# DBTable Class Documentation

The `DBTable` class provides a robust, validated way to represent database table hierarchies across different database systems. It supports various naming conventions and hierarchy levels used by different database platforms.

## Features

- **Multi-level hierarchy support**: Supports 2-4 level hierarchies (catalog.database.schema.table)
- **Parameter aliases**: Multiple ways to specify the same parameter for flexibility
- **Comprehensive validation**: Name validation, hierarchy requirements, and error handling
- **String representation**: Clean string output for use in SQL queries
- **Child table creation**: Easy creation of related tables (backups, temp tables, etc.)
- **SQLAlchemy integration**: Convert to ORM classes for database operations

## Installation

The DBTable class is part of the npd_plainerflow package:

```python
from npd_plainerflow import DBTable
```

## Basic Usage

### Creating DBTable Instances

```python
# MySQL-style (2 levels: database.table)
mysql_table = DBTable(database='ecommerce', table='users')
print(mysql_table)  # Output: ecommerce.users

# PostgreSQL-style (3 levels: database.schema.table)
postgres_table = DBTable(database='ecommerce', schema='public', table='orders')
print(postgres_table)  # Output: ecommerce.public.orders

# Databricks/Spark-style (3 levels: catalog.database.table)
spark_table = DBTable(catalog='main', database='analytics', table='events')
print(spark_table)  # Output: main.analytics.events

# Full hierarchy (4 levels: catalog.database.schema.table)
full_table = DBTable(catalog='prod', database='ecommerce', schema='public', table='customers')
print(full_table)  # Output: prod.ecommerce.public.customers

# Using views instead of tables
view_table = DBTable(database='reporting', view='monthly_sales')
print(view_table)  # Output: reporting.monthly_sales
```

### Parameter Aliases

The class supports multiple parameter names for flexibility:

```python
# Database aliases
DBTable(database='mydb', table='users')      # Standard
DBTable(database_name='mydb', table='users') # Explicit
DBTable(db='mydb', table='users')            # Short
DBTable(db_name='mydb', table='users')       # Short explicit

# Table aliases
DBTable(database='mydb', table='users')      # Standard
DBTable(database='mydb', table_name='users') # Explicit

# Schema aliases
DBTable(database='mydb', schema='public', table='users')      # Standard
DBTable(database='mydb', schema_name='public', table='users') # Explicit

# Catalog aliases
DBTable(catalog='main', database='mydb', table='users')      # Standard
DBTable(catalog_name='main', database='mydb', table='users') # Explicit

# View aliases
DBTable(database='mydb', view='user_view')      # Standard
DBTable(database='mydb', view_name='user_view') # Explicit
```

## Validation Rules

### Name Validation
- Names must start with a letter (a-z, A-Z)
- Names can contain letters, numbers, underscores (_), and dashes (-)
- Names cannot contain spaces or special characters (., @, etc.)
- Names must be 1-60 characters long
- Names cannot be empty

### Hierarchy Validation
- At least 2 different hierarchy levels are required
- Cannot specify multiple parameters from the same level (e.g., both `table` and `view`)
- Cannot specify parameters that conflict with each other

### Valid Examples
```python
DBTable(database='my_db-v2', table='user_events_2024')  # Valid
DBTable(catalog='prod', database='analytics', table='events')  # Valid
```

### Invalid Examples
```python
DBTable(table='users')  # Error: Need at least 2 levels
DBTable(table='users', view='user_view')  # Error: Same level conflict
DBTable(database='1database', table='users')  # Error: Starts with number
DBTable(database='my database', table='users')  # Error: Contains space
```

## Using in SQL Queries

DBTable instances can be used directly in f-strings and other string formatting:

```python
users_table = DBTable(database='ecommerce', table='users')
orders_table = DBTable(database='ecommerce', table='orders')

# Simple query
query = f"SELECT * FROM {users_table}"
# Output: SELECT * FROM ecommerce.users

# Join query
query = f"""
SELECT u.name, COUNT(o.id) as order_count
FROM {users_table} u
LEFT JOIN {orders_table} o ON u.id = o.user_id
GROUP BY u.id, u.name
"""

# Using .format()
query = "INSERT INTO {} (name, email) VALUES (?, ?)".format(users_table)
```

## Child Table Creation

Create related tables easily:

```python
# Original table
users_table = DBTable(database='ecommerce', schema='public', table='users')

# Create backup table
backup_table = users_table.make_child('backup')
print(backup_table)  # Output: ecommerce.public.users_backup

# Create temporary table
temp_table = users_table.create_child('temp')  # alias for make_child
print(temp_table)  # Output: ecommerce.public.users_temp

# Chain child creation
archive_table = backup_table.make_child('y2024')
print(archive_table)  # Output: ecommerce.public.users_backup_y2024

# Works with views too
sales_view = DBTable(database='reporting', view='sales_summary')
monthly_view = sales_view.make_child('monthly')
print(monthly_view)  # Output: reporting.sales_summary_monthly
```

## SQLAlchemy Integration

Convert DBTable instances to SQLAlchemy ORM classes:

```python
from sqlalchemy import create_engine

# Create engine
engine = create_engine("postgresql://user:pass@localhost/mydb")

# Create DBTable
table = DBTable(database='mydb', schema='public', table='users')

# Convert to ORM class
UserModel = table.to_orm(engine)

# Use the ORM class
session = Session(engine)
users = session.query(UserModel).all()

# Custom class name
CustomUser = table.to_orm(engine, python_class_name='CustomUser')
```

## String Representation

```python
table = DBTable(database='mydb', table='users')

# String representation (for SQL)
str(table)   # Output: 'mydb.users'

# Repr (for debugging)
repr(table)  # Output: "DBTable(database='mydb', table='users')"
```

## Error Handling

The class provides specific exception types:

```python
from npd_plainerflow import DBTableValidationError, DBTableHierarchyError

try:
    table = DBTable(database='invalid name', table='users')
except DBTableValidationError as e:
    print(f"Validation error: {e}")

try:
    table = DBTable(table='users')  # Only one level
except DBTableHierarchyError as e:
    print(f"Hierarchy error: {e}")
```

## Practical Example: Data Pipeline

```python
# Define tables for a data pipeline
raw_events = DBTable(catalog='bronze', database='events', table='raw_events')
clean_events = raw_events.make_child('clean')
aggregated_events = DBTable(catalog='silver', database='analytics', table='daily_aggregates')

# Generate SQL for the pipeline
extract_sql = f"SELECT * FROM {raw_events} WHERE date >= '2024-01-01'"

transform_sql = f"""
INSERT INTO {clean_events}
SELECT 
    event_id,
    LOWER(event_type) as event_type,
    user_id,
    timestamp
FROM {raw_events}
WHERE event_type IS NOT NULL
"""

load_sql = f"""
INSERT INTO {aggregated_events}
SELECT 
    DATE(timestamp) as event_date,
    event_type,
    COUNT(*) as event_count
FROM {clean_events}
GROUP BY DATE(timestamp), event_type
"""
```

## API Reference

### Constructor

```python
DBTable(**kwargs)
```

**Parameters:**
- `catalog` / `catalog_name`: Catalog name (optional)
- `database` / `database_name` / `db` / `db_name`: Database name
- `schema` / `schema_name`: Schema name (optional)
- `table` / `table_name`: Table name (mutually exclusive with view)
- `view` / `view_name`: View name (mutually exclusive with table)

### Properties

- `catalog`: Catalog name (str or None)
- `database`: Database name (str or None)
- `schema`: Schema name (str or None)
- `table`: Table name (str or None)
- `view`: View name (str or None)

### Methods

#### `make_child(suffix: str) -> DBTable`
Create a child table by appending a suffix to the table/view name.

#### `create_child(suffix: str) -> DBTable`
Alias for `make_child()`.

#### `to_orm(engine, python_class_name: str = None) -> type`
Convert to SQLAlchemy ORM class.

**Parameters:**
- `engine`: SQLAlchemy engine for table reflection
- `python_class_name`: Custom class name (optional)

**Returns:** SQLAlchemy ORM class

### String Methods

#### `__str__() -> str`
Returns dot-separated hierarchy string for use in SQL.

#### `__repr__() -> str`
Returns constructor representation for debugging.

## Exception Classes

### `DBTableError`
Base exception class for all DBTable errors.

### `DBTableValidationError`
Raised when parameter validation fails (invalid names, unknown parameters, etc.).

### `DBTableHierarchyError`
Raised when hierarchy requirements are not met (too few levels, conflicting levels, etc.).

## Dependencies

- **Required**: None (core functionality)
- **Optional**: SQLAlchemy (for ORM integration)

## Testing

The DBTable class includes comprehensive tests covering:
- Basic creation and validation
- Parameter aliases
- Hierarchy validation
- String representation
- Child creation
- SQLAlchemy integration
- Edge cases and error handling

Run tests with:
```bash
python -m pytest tests/test_dbtable.py -v
```

## Examples

See `examples/dbtable_usage.py` for comprehensive usage examples.
