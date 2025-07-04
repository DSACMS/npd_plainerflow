#!/usr/bin/env python3
"""
PlainerFlow Pipeline Example

This script demonstrates a complete data transformation pipeline using PlainerFlow components:
- CredentialFinder for database connections
- DBTable for table references
- FrostDict for SQL configuration
- SQLoopcicle for SQL execution
- InLaw for data validation

Run this script to see PlainerFlow in action with the example CSV data.
"""

import plainerflow
from plainerflow import CredentialFinder, DBTable, FrostDict, SQLoopcicle, InLaw
import pandas as pd
import sqlalchemy
from pathlib import Path

def main():
    print("🚀 PlainerFlow Pipeline Example")
    print("=" * 50)
    
    # Step 1: Get database connection using PostgreSQL testcontainer
    print("Step 1: Connecting to database...")
    try:
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
    customers_table = DBTable(schema='public', table='customers')
    orders_table = DBTable(schema='public', table='orders')
    customer_summary_table = DBTable(schema='public', table='customer_summary')
    
    print(f"Will create tables: {customers_table}, {orders_table}, {customer_summary_table}")
    
    # Step 3: Load CSV data into pandas DataFrames
    print("\nStep 3: Loading CSV data...")
    try:
        customers_df = pd.read_csv('readme_example_data/customers.csv')
        orders_df = pd.read_csv('readme_example_data/orders.csv')
        print(f"✅ Loaded {len(customers_df)} customers and {len(orders_df)} orders")
    except FileNotFoundError as e:
        print(f"❌ Error: Could not find CSV files. Make sure you're running from the project root directory.")
        print(f"   Expected files: readme_example_data/customers.csv, readme_example_data/orders.csv")
        return
    
    # Step 4: Import data to PostgreSQL
    print("\nStep 4: Importing data to database...")
    customers_df.to_sql('customers', engine, if_exists='replace', index=False, schema='public')
    orders_df.to_sql('orders', engine, if_exists='replace', index=False, schema='public')
    print("✅ CSV data imported to PostgreSQL")
    
    # Step 5: Define transformation SQL using FrostDict
    print("\nStep 5: Defining SQL transformations...")
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
    print("\nStep 6: Running data transformations...")
    SQLoopcicle.run_sql_loop(transformation_sql, engine)
    
    # Step 7: Create validation tests using InLaw
    print("\nStep 7: Defining validation tests...")
    
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
            
            null_count = gdf.iloc[0]['null_count']
            
            if null_count == 0:
                return True
            return f"Found {null_count} null names in customer summary"
    
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
            
            invalid_count = gdf.iloc[0]['invalid_count']
            
            if invalid_count == 0:
                return True
            return f"Found {invalid_count} customers with orders but non-positive spending"
    
    # Step 8: Run validation tests
    print("\nStep 8: Running data validation tests...")
    test_results = InLaw.run_all(engine)
    
    # Step 9: Display sample results
    print("\nStep 9: Displaying sample results...")
    print("📊 Customer Summary Sample:")
    with engine.connect() as conn:
        sample_data = pd.read_sql_query(f"SELECT * FROM {customer_summary_table} LIMIT 3", conn)
        print(sample_data.to_string(index=False))
    
    print("\n📊 Order Metrics:")
    with engine.connect() as conn:
        metrics_data = pd.read_sql_query("SELECT * FROM order_metrics ORDER BY order_count DESC", conn)
        print(metrics_data.to_string(index=False))
    
    # Final summary
    print("\n" + "=" * 50)
    print("✅ Pipeline completed successfully!")
    print(f"   - Imported {len(customers_df)} customers and {len(orders_df)} orders")
    print(f"   - Created {customer_summary_table} and order_metrics tables")
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
        print("pip install plainerflow pandas great-expectations")
        raise
