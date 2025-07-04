"""
Example usage of FrostDict - A frozen dictionary for SQL templates.

This example demonstrates the key features of FrostDict:
1. Feels like a normal dict for reads
2. Prevents accidental re-assignment of top-level keys
3. Allows first insertion of new keys
4. Nested values remain mutable
5. Integration with SQLoopcicle
"""

import plainerflow
from sqlalchemy import create_engine


def main():
    print("=== FrostDict Usage Examples ===\n")
    
    # Example 1: Module load time definition (as specified)
    schema_name = 'test_schema'
    QUERIES = plainerflow.FrostDict({
        "create_schema": f"CREATE SCHEMA IF NOT EXISTS {schema_name}",
        "book_by_id": "SELECT * FROM books WHERE id = :book_id",
        "all_authors": "SELECT * FROM authors",
    })
    
    print("1. Module load time definition:")
    print(f"   create_schema: {QUERIES['create_schema']}")
    print(f"   book_by_id: {QUERIES['book_by_id']}")
    print(f"   all_authors: {QUERIES['all_authors']}")
    
    # Example 2: Incremental building pattern
    print("\n2. Incremental building pattern:")
    sql = plainerflow.FrostDict()
    sql['create_table'] = "CREATE TABLE users (id INTEGER, name TEXT)"
    sql['insert_user'] = "INSERT INTO users (name) VALUES (:name)"
    sql['select_all'] = "SELECT * FROM users"
    
    print(f"   Built incrementally: {list(sql.keys())}")
    
    # Example 3: Demonstrating frozen behavior
    print("\n3. Frozen behavior demonstration:")
    try:
        QUERIES["create_schema"] = "different query"
        print("   ERROR: Should have raised FrozenKeyError!")
    except plainerflow.FrozenKeyError as e:
        print(f"   ✓ Correctly prevented reassignment: {e}")
    
    try:
        sql['create_table'] = "CREATE TABLE different_table (id INTEGER)"
        print("   ERROR: Should have raised FrozenKeyError!")
    except plainerflow.FrozenKeyError as e:
        print(f"   ✓ Correctly prevented reassignment: {e}")
    
    # Example 4: Nested mutability
    print("\n4. Nested mutability:")
    config_dict = plainerflow.FrostDict({
        'database': {'host': 'localhost', 'port': 5432},
        'queries': ['SELECT 1', 'SELECT 2']
    })
    
    print(f"   Original database config: {config_dict['database']}")
    
    # Modify nested values (this should work)
    config_dict['database']['host'] = 'remote-server'
    config_dict['database']['timeout'] = 30
    config_dict['queries'].append('SELECT 3')
    
    print(f"   Modified database config: {config_dict['database']}")
    print(f"   Modified queries: {config_dict['queries']}")
    
    # But top-level keys are still frozen
    try:
        config_dict['database'] = {'completely': 'different'}
        print("   ERROR: Should have raised FrozenKeyError!")
    except plainerflow.FrozenKeyError as e:
        print(f"   ✓ Top-level key still frozen: {e}")
    
    # Example 5: Integration with SQLoopcicle
    print("\n5. Integration with SQLoopcicle:")
    engine = create_engine("sqlite:///:memory:")
    
    sql_workflow = plainerflow.FrostDict({
        "create_users": "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)",
        "create_orders": "CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, amount REAL)",
        "insert_sample_user": "INSERT INTO users (name, email) VALUES ('Alice', 'alice@example.com')",
        "insert_sample_order": "INSERT INTO orders (user_id, amount) VALUES (1, 99.99)"
    })
    
    print("   Dry-run mode:")
    plainerflow.SQLoopcicle.run_sql_loop(sql_workflow, engine, is_just_print=True)
    
    print("\n   Execution mode:")
    plainerflow.SQLoopcicle.run_sql_loop(sql_workflow, engine, is_just_print=False)
    
    # Verify the SQL was executed
    from sqlalchemy import text
    with engine.begin() as conn:
        user_count = conn.execute(text("SELECT COUNT(*) FROM users")).scalar()
        order_count = conn.execute(text("SELECT COUNT(*) FROM orders")).scalar()
        print(f"   ✓ Created {user_count} users and {order_count} orders")
    
    # Example 6: Hashability (when all values are hashable)
    print("\n6. Hashability:")
    hashable_dict = plainerflow.FrostDict({
        'string': 'value',
        'number': 42,
        'tuple': (1, 2, 3),
        'boolean': True
    })
    
    try:
        hash_value = hash(hashable_dict)
        print(f"   ✓ Hashable FrostDict hash: {hash_value}")
        
        # Can be used as dict key
        container = {hashable_dict: 'stored_value'}
        print(f"   ✓ Used as dict key: {container[hashable_dict]}")
        
    except TypeError as e:
        print(f"   ✗ Not hashable: {e}")
    
    # Example 7: Non-hashable when containing mutable values
    print("\n7. Non-hashable with mutable values:")
    non_hashable_dict = plainerflow.FrostDict({
        'list': [1, 2, 3],  # Lists are not hashable
        'string': 'value'
    })
    
    try:
        hash(non_hashable_dict)
        print("   ERROR: Should have raised TypeError!")
    except TypeError as e:
        print(f"   ✓ Correctly not hashable: {e}")
    
    print("\n=== FrostDict Examples Complete ===")


if __name__ == '__main__':
    main()
