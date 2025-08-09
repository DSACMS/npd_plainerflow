#!/usr/bin/env python3
"""
Example usage of SQLoopcicle class.

This example demonstrates how to use SQLoopcicle for both dry-run and execution modes
with a SQLite database.
"""

from sqlalchemy import create_engine, text
from npd_plainerflow import SQLoopcicle

def main():
    """Demonstrate SQLoopcicle usage."""
    
    # Create an in-memory SQLite database for demonstration
    engine = create_engine("sqlite:///:memory:")
    
    # Define a series of SQL operations
    sql_operations = {
        "create_users_table": """
            CREATE TABLE users (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                email TEXT UNIQUE,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """,
        "create_orders_table": """
            CREATE TABLE orders (
                id INTEGER PRIMARY KEY,
                user_id INTEGER,
                product_name TEXT NOT NULL,
                amount DECIMAL(10,2),
                order_date DATETIME DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users (id)
            )
        """,
        "insert_sample_users": """
            INSERT INTO users (name, email) VALUES 
            ('Alice Johnson', 'alice@example.com'),
            ('Bob Smith', 'bob@example.com'),
            ('Carol Davis', 'carol@example.com')
        """,
        "insert_sample_orders": """
            INSERT INTO orders (user_id, product_name, amount) VALUES 
            (1, 'Laptop', 999.99),
            (1, 'Mouse', 29.99),
            (2, 'Keyboard', 79.99),
            (3, 'Monitor', 299.99)
        """,
        "create_summary_view": """
            CREATE VIEW user_order_summary AS
            SELECT 
                u.name,
                u.email,
                COUNT(o.id) as order_count,
                SUM(o.amount) as total_spent
            FROM users u
            LEFT JOIN orders o ON u.id = o.user_id
            GROUP BY u.id, u.name, u.email
        """
    }
    
    print("=== SQLoopcicle Demo ===\n")
    
    # First, demonstrate dry-run mode
    print("1. DRY-RUN MODE (no actual execution):")
    print("-" * 50)
    SQLoopcicle.run_sql_loop(sql_operations, engine, is_just_print=True)
    
    print("\n" + "=" * 60 + "\n")
    
    # Now execute the SQL for real
    print("2. EXECUTION MODE (actual database operations):")
    print("-" * 50)
    SQLoopcicle.run_sql_loop(sql_operations, engine, is_just_print=False)
    
    print("\n" + "=" * 60 + "\n")
    
    # Verify the results by querying the database
    print("3. VERIFICATION - Querying the created database:")
    print("-" * 50)
    
    verification_queries = {
        "count_users": "SELECT COUNT(*) as user_count FROM users",
        "count_orders": "SELECT COUNT(*) as order_count FROM orders", 
        "show_summary": "SELECT * FROM user_order_summary ORDER BY total_spent DESC"
    }
    
    # Execute verification queries and show results
    with engine.begin() as conn:
        for query_name, query_sql in verification_queries.items():
            print(f"\n▶ {query_name}: {query_sql}")
            result = conn.execute(text(query_sql)).fetchall()
            for row in result:
                print(f"  {row}")
    
    print("\n" + "=" * 60)
    print("Demo complete! SQLoopcicle successfully executed all SQL operations.")

if __name__ == "__main__":
    main()
