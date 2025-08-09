#!/usr/bin/env python3
"""
npd_plainerflow Pipeline Example

This script demonstrates a complete data transformation pipeline using npd_plainerflow components:
- CredentialFinder for database connections
- DBTable for table references
- FrostDict for SQL configuration
- SQLoopcicle for SQL execution
- InLaw for data validation

Run this script to see npd_plainerflow in action with the example CSV data.
"""

import npd_plainerflow
from npd_plainerflow import CredentialFinder, DBTable, FrostDict, SQLoopcicle, InLaw
import pandas as pd
import sqlalchemy
from pathlib import Path

def main():
    print("=== npd_plainerflow Pipeline Example Program ===")
    print("=" * 50)
    
    # Step 1: Get database connection using PostgreSQL testcontainer
    print("Step 1: Connecting to database...")
    try:
        # We want to use a real database for testing, so we have use a container version of postgres designed for this purpose. 
        from testcontainers.postgres import PostgresContainer
        postgres = PostgresContainer("postgres:15")
        postgres.start()
        engine = sqlalchemy.create_engine(postgres.get_connection_url())
        print("✅ Connected to PostgreSQL test container")
    except ImportError:
        print("❌ testcontainers not available, falling back to CredentialFinder")
        engine = CredentialFinder.detect_config(verbose=True)
    
    # Step 2: Define target tables using DBTable with proper hierarchy (schema.table)
    print("\nStep 2: Defining table references...")
    customers_DBTable = DBTable(schema='public', table='customers')
    orders_DBTable = DBTable(schema='public', table='orders')
    customer_summary_DBTable = DBTable(schema='public', table='customer_summary')
    
    print(f"Will create tables: {customers_DBTable}, {orders_DBTable}, {customer_summary_DBTable}")

    # Step 3: Define data loading SQL using FrostDict
    print("\nStep 3: Defining data loading SQL...")
    
    # Get absolute paths to CSV files
    csv_dir = Path(__file__).parent / 'readme_example_data'
    customers_csv_path = csv_dir / 'customers.csv'
    orders_csv_path = csv_dir / 'orders.csv'
    
    # Check if CSV files exist
    if not customers_csv_path.exists() or not orders_csv_path.exists():
        print(f"❌ Error: Could not find CSV files. Make sure you're running from the project root directory.")
        print(f"   Expected files: {customers_csv_path}, {orders_csv_path}")
        return
    
    # Read CSV data to generate INSERT statements
    customers_df = pd.read_csv(customers_csv_path)
    orders_df = pd.read_csv(orders_csv_path)
    
    # Generate INSERT statements for customers
    customers_inserts = []
    for _, row in customers_df.iterrows():
        customers_inserts.append(f"""
            INSERT INTO {customers_DBTable} (customer_id, first_name, last_name, email, registration_date, status)
            VALUES ({row['customer_id']}, '{row['first_name']}', '{row['last_name']}', '{row['email']}', '{row['registration_date']}', '{row['status']}');
        """)
    
    # Generate INSERT statements for orders
    orders_inserts = []
    for _, row in orders_df.iterrows():
        orders_inserts.append(f"""
            INSERT INTO {orders_DBTable} (order_id, customer_id, product_name, quantity, unit_price, order_date, order_status)
            VALUES ({row['order_id']}, {row['customer_id']}, '{row['product_name']}', {row['quantity']}, {row['unit_price']}, '{row['order_date']}', '{row['order_status']}');
        """)
    
    # Step 4: Define complete SQL pipeline with single FrostDict
    print("\nStep 4: Defining complete SQL pipeline...")
    
    # Single FrostDict containing all SQL operations
    complete_sql_pipeline = FrostDict({
        # Data loading operations
        'create_customers_DBTable': f"""
            DROP TABLE IF EXISTS {customers_DBTable} CASCADE;
            CREATE TABLE {customers_DBTable} (
                customer_id INTEGER PRIMARY KEY,
                first_name VARCHAR(100),
                last_name VARCHAR(100),
                email VARCHAR(255),
                registration_date DATE,
                status VARCHAR(50)
            );
        """,
        
        'create_orders_DBTable': f"""
            DROP TABLE IF EXISTS {orders_DBTable} CASCADE;
            CREATE TABLE {orders_DBTable} (
                order_id INTEGER PRIMARY KEY,
                customer_id INTEGER,
                product_name VARCHAR(255),
                quantity INTEGER,
                unit_price DECIMAL(10,2),
                order_date DATE,
                order_status VARCHAR(50),
                FOREIGN KEY (customer_id) REFERENCES {customers_DBTable}(customer_id)
            );
        """,
        
        'load_customers_data': '\n'.join(customers_inserts),
        'load_orders_data': '\n'.join(orders_inserts),
        
        # Transformation operations
        'create_customer_summary': f"""
            CREATE TABLE IF NOT EXISTS {customer_summary_DBTable} AS
            SELECT 
                c.customer_id,
                c.first_name || ' ' || c.last_name as full_name,
                c.email,
                c.status as customer_status,
                COUNT(o.order_id) as total_orders,
                COALESCE(SUM(o.quantity * o.unit_price), 0) as total_spent,
                MAX(o.order_date) as last_order_date
            FROM {customers_DBTable} c
            LEFT JOIN {orders_DBTable} o ON c.customer_id = o.customer_id
            GROUP BY c.customer_id, c.first_name, c.last_name, c.email, c.status
        """,
        
        'create_order_metrics': f"""
            CREATE TABLE IF NOT EXISTS order_metrics AS
            SELECT 
                order_status,
                COUNT(*) as order_count,
                AVG(quantity * unit_price) as avg_order_value,
                SUM(quantity * unit_price) as total_value
            FROM {orders_DBTable}
            GROUP BY order_status
        """,
        
        # Display queries
        'customer_summary_sample': f"SELECT * FROM {customer_summary_DBTable} LIMIT 3",
        'order_metrics_report': "SELECT * FROM order_metrics ORDER BY order_count DESC"
    })
    



    # Step 5: Execute complete SQL pipeline with single run_sql_loop call
    print("\nStep 5: Executing complete SQL pipeline...")
    SQLoopcicle.run_sql_loop(complete_sql_pipeline, engine, select_display_rows=10)
    
    
    # Step 6: Create validation tests using InLaw
    print("\nStep 6: Defining validation tests...")
    
    class ValidateCustomerSummaryRowCount(InLaw):
        title = "Customer summary should have same number of rows as customers"
        
        @staticmethod
        def run(engine):
            # Get the expected row count from the customers table
            with engine.connect() as conn:
                expected_rows = conn.execute(sqlalchemy.text(f"SELECT COUNT(*) FROM {customers_DBTable}")).scalar_one()
    
            # Use Great Expectations to check the row count of the summary table
            validation_gx_df = InLaw.sql_to_gx_df(sql=f"SELECT * FROM {customer_summary_DBTable}", engine=engine)
            result = validation_gx_df.expect_table_row_count_to_equal(value=expected_rows)
            
            if result.success:
                return True
            
            observed_count = result.result.get("observed_value", "N/A")
            return f"Row count mismatch: expected {expected_rows} rows, but found {observed_count}"
    
    class ValidateNoNullCustomerNames(InLaw):
        title = "Customer summary should have no null names"
        
        @staticmethod
        def run(engine):
            sql = f"SELECT full_name FROM {customer_summary_DBTable}"
            validation_gx_df = InLaw.sql_to_gx_df(sql=sql, engine=engine)
            result = validation_gx_df.expect_column_values_to_not_be_null(column='full_name')
            
            if result.success:
                return True
            
            unexpected_count = result.result.get("unexpected_count", "N/A")
            return f"Found {unexpected_count} null names in customer summary"
    
    class ValidateTotalSpentIsPositive(InLaw):
        title = "Active customers with orders should have positive total_spent"
        
        @staticmethod
        def run(engine):
            sql = f"SELECT total_spent FROM {customer_summary_DBTable} WHERE total_orders > 0"
            validation_gx_df = InLaw.sql_to_gx_df(sql=sql, engine=engine)
            result = validation_gx_df.expect_column_values_to_be_greater_than(column='total_spent', value=0)
            
            if result.success:
                return True
                
            unexpected_count = result.result.get("unexpected_count", "N/A")
            return f"Found {unexpected_count} customers with orders but non-positive spending"
    
    # Step 7: Run validation tests using InLaw
    print("\nStep 7: Running data validation tests...")
    test_results = InLaw.run_all(engine=engine)
    
    # Final summary
    print("\n" + "=" * 50)
    print("✅ Pipeline completed successfully!")
    print(f"   - Validation results: {test_results['passed']} passed, {test_results['failed']} failed")
    
    if test_results['failed'] > 0 or test_results['errors'] > 0:
        print("⚠️  Some validation tests failed. Check the output above for details.")
    else:
        print("🎉 All validation tests passed!")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\n❌ Pipeline failed with error: {e}")
        print("\nMake sure you have installed the required dependencies:")
        print("pip install npd_plainerflow pandas great-expectations")
        raise
