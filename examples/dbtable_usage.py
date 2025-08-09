"""
Example usage of the DBTable class.

This example demonstrates the key features of DBTable including:
- Basic table creation with different hierarchy levels
- Parameter aliases
- String representation for SQL queries
- Child table creation
- SQLAlchemy ORM integration (when dependencies are available)
"""

import sys
import os

# Add the parent directory to the path so we can import npd_plainerflow
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from npd_plainerflow import DBTable, DBTableValidationError, DBTableHierarchyError


def basic_usage_examples():
    """Demonstrate basic DBTable usage."""
    print("=== Basic DBTable Usage ===")
    
    # MySQL-style (2 levels)
    mysql_table = DBTable(database='ecommerce', table='users')
    print(f"MySQL style: {mysql_table}")
    print(f"Repr: {repr(mysql_table)}")
    
    # PostgreSQL-style (3 levels)
    postgres_table = DBTable(database='ecommerce', schema='public', table='orders')
    print(f"PostgreSQL style: {postgres_table}")
    
    # Databricks/Spark-style (3 levels)
    spark_table = DBTable(catalog='main', database='analytics', table='events')
    print(f"Spark style: {spark_table}")
    
    # Full hierarchy (4 levels)
    full_table = DBTable(catalog='prod', database='ecommerce', schema='public', table='customers')
    print(f"Full hierarchy: {full_table}")
    
    # Using view instead of table
    view_table = DBTable(database='reporting', view='monthly_sales')
    print(f"With view: {view_table}")
    print()


def parameter_aliases_examples():
    """Demonstrate parameter aliases."""
    print("=== Parameter Aliases ===")
    
    # Different ways to specify database
    db1 = DBTable(database='mydb', table='users')
    db2 = DBTable(database_name='mydb', table='users')
    db3 = DBTable(db='mydb', table='users')
    db4 = DBTable(db_name='mydb', table='users')
    
    print(f"database: {db1}")
    print(f"database_name: {db2}")
    print(f"db: {db3}")
    print(f"db_name: {db4}")
    
    # Different ways to specify table
    tbl1 = DBTable(database='mydb', table='users')
    tbl2 = DBTable(database='mydb', table_name='users')
    
    print(f"table: {tbl1}")
    print(f"table_name: {tbl2}")
    print()


def sql_query_usage():
    """Demonstrate using DBTable in SQL queries."""
    print("=== SQL Query Usage ===")
    
    users_table = DBTable(database='ecommerce', table='users')
    orders_table = DBTable(database='ecommerce', table='orders')
    
    # Using in f-strings for SQL queries
    query1 = f"SELECT * FROM {users_table}"
    print(f"Simple query: {query1}")
    
    query2 = f"""
    SELECT u.name, COUNT(o.id) as order_count
    FROM {users_table} u
    LEFT JOIN {orders_table} o ON u.id = o.user_id
    GROUP BY u.id, u.name
    """
    print(f"Join query: {query2.strip()}")
    
    # Using with .format()
    query3 = "INSERT INTO {} (name, email) VALUES (?, ?)".format(users_table)
    print(f"Insert query: {query3}")
    print()


def child_creation_examples():
    """Demonstrate child table creation."""
    print("=== Child Table Creation ===")
    
    # Original table
    users_table = DBTable(database='ecommerce', schema='public', table='users')
    print(f"Original table: {users_table}")
    
    # Create backup table
    backup_table = users_table.make_child('backup')
    print(f"Backup table: {backup_table}")
    
    # Create temporary table
    temp_table = users_table.create_child('temp')  # alias for make_child
    print(f"Temp table: {temp_table}")
    
    # Chain child creation
    archive_table = backup_table.make_child('y2024')
    print(f"Archive table: {archive_table}")
    
    # Works with views too
    sales_view = DBTable(database='reporting', view='sales_summary')
    monthly_view = sales_view.make_child('monthly')
    print(f"Monthly view: {monthly_view}")
    print()


def validation_examples():
    """Demonstrate validation and error handling."""
    print("=== Validation Examples ===")
    
    # Valid names
    try:
        valid_table = DBTable(database='my_db-v2', table='user_events_2024')
        print(f"Valid complex names: {valid_table}")
    except Exception as e:
        print(f"Unexpected error: {e}")
    
    # Invalid: not enough parameters
    try:
        DBTable(table='users')
    except DBTableHierarchyError as e:
        print(f"Hierarchy error: {e}")
    
    # Invalid: same level parameters
    try:
        DBTable(table='users', view='user_view')
    except DBTableHierarchyError as e:
        print(f"Same level error: {e}")
    
    # Invalid: name starts with number
    try:
        DBTable(database='1database', table='users')
    except DBTableValidationError as e:
        print(f"Validation error: {e}")
    
    # Invalid: special characters
    try:
        DBTable(database='my database', table='users')
    except DBTableValidationError as e:
        print(f"Special character error: {e}")
    
    # Invalid: too long
    try:
        long_name = 'a' * 61
        DBTable(database=long_name, table='users')
    except DBTableValidationError as e:
        print(f"Length error: {e}")
    print()


def sqlalchemy_integration_example():
    """Demonstrate SQLAlchemy integration (if available)."""
    print("=== SQLAlchemy Integration ===")
    
    try:
        from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String
        
        # Create in-memory SQLite database for demo
        engine = create_engine("sqlite:///:memory:")
        
        # Create a test table
        metadata = MetaData()
        test_table = Table(
            'users',
            metadata,
            Column('id', Integer, primary_key=True),
            Column('name', String(50)),
            Column('email', String(100))
        )
        metadata.create_all(engine)
        
        # Create DBTable instance
        db_table = DBTable(database='testdb', table='users')
        print(f"DBTable: {db_table}")
        
        # Convert to ORM (this would work with a real database)
        try:
            orm_class = db_table.to_orm(engine)
            print(f"Generated ORM class: {orm_class.__name__}")
            if hasattr(orm_class, '__table__'):
                print(f"ORM table name: {orm_class.__table__.name}")
        except Exception as e:
            print(f"ORM creation note: {e}")
            print("(This is expected in the demo - would work with proper table reflection)")
        
    except ImportError:
        print("SQLAlchemy not available - install with: pip install sqlalchemy")
    print()


def practical_example():
    """Show a practical example of using DBTable in a data pipeline."""
    print("=== Practical Example: Data Pipeline ===")
    
    # Define tables for a data pipeline
    raw_events = DBTable(catalog='bronze', database='events', table='raw_events')
    clean_events = raw_events.make_child('clean')
    aggregated_events = DBTable(catalog='silver', database='analytics', table='daily_aggregates')
    
    print("Data pipeline tables:")
    print(f"1. Raw data: {raw_events}")
    print(f"2. Cleaned data: {clean_events}")
    print(f"3. Aggregated data: {aggregated_events}")
    
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
    
    print("\nGenerated SQL:")
    print("Extract:", extract_sql)
    print("Transform:", transform_sql.strip())
    print("Load:", load_sql.strip())
    print()


if __name__ == '__main__':
    print("DBTable Usage Examples")
    print("=" * 50)
    
    basic_usage_examples()
    parameter_aliases_examples()
    sql_query_usage()
    child_creation_examples()
    validation_examples()
    sqlalchemy_integration_example()
    practical_example()
    
    print("All examples completed!")
