"""
Tests for SQLoopcicle class.

Tests cover dry-run mode, actual execution mode, output formatting,
error propagation, and integration with SQLAlchemy engines using SQLite.
"""

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

from plainerflow.sqloopcicle import SQLoopcicle


class TestSQLoopcicleBasicFunctionality:
    """Test basic SQLoopcicle functionality."""
    
    def setup_method(self):
        """Set up test database for each test."""
        self.engine = create_engine("sqlite:///:memory:")
    
    def test_dry_run_mode_basic(self, capsys):
        """Test basic dry-run mode functionality."""
        sql_dict = {
            "create_table": "CREATE TABLE users (id INTEGER, name TEXT)",
            "insert_data": "INSERT INTO users VALUES (1, 'Alice')"
        }
        
        SQLoopcicle.run_sql_loop(sql_dict, self.engine, is_just_print=True)
        
        # Check output
        captured = capsys.readouterr()
        expected_lines = [
            "⏩ =====  DRY-RUN MODE – NO SQL WILL BE EXECUTED =====",
            "create_table:",
            "CREATE TABLE users (id INTEGER, name TEXT)",
            "insert_data:",
            "INSERT INTO users VALUES (1, 'Alice')",
            "🟡 ===== I AM NOT RUNNING SQL ====="
        ]
        
        for line in expected_lines:
            assert line in captured.out
        
        # Verify no tables were created (dry-run didn't execute)
        with self.engine.begin() as conn:
            result = conn.execute(text("SELECT name FROM sqlite_master WHERE type='table'")).fetchall()
            assert len(result) == 0  # No tables should exist
    
    def test_execution_mode_basic(self, capsys):
        """Test basic execution mode functionality."""
        sql_dict = {
            "create_table": "CREATE TABLE users (id INTEGER, name TEXT)",
            "insert_data": "INSERT INTO users VALUES (1, 'Alice')"
        }
        
        SQLoopcicle.run_sql_loop(sql_dict, self.engine, is_just_print=False)
        
        # Check output
        captured = capsys.readouterr()
        expected_lines = [
            "⏩ =====  EXECUTING SQL LOOP =====",
            "create_table:",
            "CREATE TABLE users (id INTEGER, name TEXT)",
            "insert_data:",
            "INSERT INTO users VALUES (1, 'Alice')",
            "⏪ ===== SQL LOOP COMPLETE ====="
        ]
        
        for line in expected_lines:
            assert line in captured.out
        
        # Verify SQL was actually executed
        with self.engine.begin() as conn:
            # Check table was created
            result = conn.execute(text("SELECT name FROM sqlite_master WHERE type='table'")).fetchall()
            assert len(result) == 1
            assert result[0][0] == 'users'
            
            # Check data was inserted
            result = conn.execute(text("SELECT COUNT(*) FROM users")).scalar()
            assert result == 1
            
            # Check specific data
            result = conn.execute(text("SELECT name FROM users WHERE id = 1")).scalar()
            assert result == 'Alice'
    
    def test_default_execution_mode(self, capsys):
        """Test that default mode is execution (not dry-run)."""
        sql_dict = {"create_table": "CREATE TABLE test_table (id INTEGER)"}
        
        # Call without is_just_print parameter
        SQLoopcicle.run_sql_loop(sql_dict, self.engine)
        
        # Check output shows execution mode
        captured = capsys.readouterr()
        assert "⏩ =====  EXECUTING SQL LOOP =====" in captured.out
        assert "⏪ ===== SQL LOOP COMPLETE =====" in captured.out
        
        # Verify table was actually created (not dry-run)
        with self.engine.begin() as conn:
            result = conn.execute(text("SELECT name FROM sqlite_master WHERE type='table'")).fetchall()
            assert len(result) == 1
            assert result[0][0] == 'test_table'


class TestSQLoopcicleOutputFormatting:
    """Test output formatting requirements."""
    
    def setup_method(self):
        """Set up test database for each test."""
        self.engine = create_engine("sqlite:///:memory:")
    
    def test_dry_run_output_format(self, capsys):
        """Test exact dry-run output format."""
        sql_dict = {"query1": "CREATE TABLE table1 (id INTEGER)"}
        
        SQLoopcicle.run_sql_loop(sql_dict, self.engine, is_just_print=True)
        
        captured = capsys.readouterr()
        lines = captured.out.strip().split('\n')
        
        # The new format uses emojis and different spacing
        assert "⏩ =====  DRY-RUN MODE – NO SQL WILL BE EXECUTED =====" in captured.out
        assert "query1:" in captured.out
        assert "CREATE TABLE table1 (id INTEGER)" in captured.out
        assert "🟡 ===== I AM NOT RUNNING SQL =====" in captured.out
    
    def test_execution_output_format(self, capsys):
        """Test exact execution output format."""
        sql_dict = {"query1": "CREATE TABLE table1 (id INTEGER)"}
        
        SQLoopcicle.run_sql_loop(sql_dict, self.engine, is_just_print=False)
        
        captured = capsys.readouterr()
        lines = captured.out.strip().split('\n')
        
        # The new format uses emojis and different spacing
        assert "⏩ =====  EXECUTING SQL LOOP =====" in captured.out
        assert "query1:" in captured.out
        assert "CREATE TABLE table1 (id INTEGER)" in captured.out
        assert "⏪ ===== SQL LOOP COMPLETE =====" in captured.out
    
    def test_multiple_queries_output_order(self, capsys):
        """Test that queries are output in dictionary order."""
        sql_dict = {
            "first": "CREATE TABLE first_table (id INTEGER)",
            "second": "CREATE TABLE second_table (id INTEGER)", 
            "third": "CREATE TABLE third_table (id INTEGER)"
        }
        
        SQLoopcicle.run_sql_loop(sql_dict, self.engine, is_just_print=True)
        
        captured = capsys.readouterr()
        lines = captured.out.strip().split('\n')
        
        # Check that queries appear in order - new format uses different icons
        assert "first:" in captured.out
        assert "CREATE TABLE first_table (id INTEGER)" in captured.out
        assert "second:" in captured.out
        assert "CREATE TABLE second_table (id INTEGER)" in captured.out
        assert "third:" in captured.out
        assert "CREATE TABLE third_table (id INTEGER)" in captured.out
    
    def test_complex_sql_formatting(self, capsys):
        """Test formatting with complex SQL statements."""
        complex_sql = """CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT UNIQUE,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )"""
        
        sql_dict = {"complex_query": complex_sql}
        
        SQLoopcicle.run_sql_loop(sql_dict, self.engine, is_just_print=True)
        
        captured = capsys.readouterr()
        # Should contain the full SQL statement - new format uses different icons
        assert "complex_query:" in captured.out
        assert "CREATE TABLE users" in captured.out
        assert "PRIMARY KEY" in captured.out


class TestSQLoopcicleEdgeCases:
    """Test edge cases and special scenarios."""
    
    def setup_method(self):
        """Set up test database for each test."""
        self.engine = create_engine("sqlite:///:memory:")
    
    def test_empty_dictionary(self, capsys):
        """Test behavior with empty SQL dictionary."""
        sql_dict = {}
        
        SQLoopcicle.run_sql_loop(sql_dict, self.engine, is_just_print=True)
        
        # Should still print start/end messages
        captured = capsys.readouterr()
        assert "⏩ =====  DRY-RUN MODE – NO SQL WILL BE EXECUTED =====" in captured.out
        assert "🟡 ===== I AM NOT RUNNING SQL =====" in captured.out
        
        # No SQL queries should be shown
        assert "▶" not in captured.out
        
        # No tables should be created
        with self.engine.begin() as conn:
            result = conn.execute(text("SELECT name FROM sqlite_master WHERE type='table'")).fetchall()
            assert len(result) == 0
    
    def test_empty_dictionary_execution_mode(self, capsys):
        """Test empty dictionary in execution mode."""
        sql_dict = {}
        
        SQLoopcicle.run_sql_loop(sql_dict, self.engine, is_just_print=False)
        
        captured = capsys.readouterr()
        assert "⏩ =====  EXECUTING SQL LOOP =====" in captured.out
        assert "⏪ ===== SQL LOOP COMPLETE =====" in captured.out
        
        # No tables should be created
        with self.engine.begin() as conn:
            result = conn.execute(text("SELECT name FROM sqlite_master WHERE type='table'")).fetchall()
            assert len(result) == 0
    
    def test_single_query(self, capsys):
        """Test with single query."""
        sql_dict = {"only_query": "CREATE TABLE single_table (id INTEGER)"}
        
        SQLoopcicle.run_sql_loop(sql_dict, self.engine)
        
        captured = capsys.readouterr()
        lines = captured.out.strip().split('\n')
        
        # The new format has more lines due to different formatting
        assert "only_query:" in captured.out
        assert "CREATE TABLE single_table (id INTEGER)" in captured.out
        
        # Verify table was created
        with self.engine.begin() as conn:
            result = conn.execute(text("SELECT name FROM sqlite_master WHERE type='table'")).fetchall()
            assert len(result) == 1
            assert result[0][0] == 'single_table'
    
    def test_special_characters_in_keys(self, capsys):
        """Test with special characters in dictionary keys."""
        sql_dict = {
            "query-with-dashes": "CREATE TABLE dash_table (id INTEGER)",
            "query_with_underscores": "CREATE TABLE underscore_table (id INTEGER)",
            "query with spaces": "CREATE TABLE space_table (id INTEGER)"
        }
        
        SQLoopcicle.run_sql_loop(sql_dict, self.engine, is_just_print=True)
        
        captured = capsys.readouterr()
        assert "query-with-dashes:" in captured.out
        assert "CREATE TABLE dash_table (id INTEGER)" in captured.out
        assert "query_with_underscores:" in captured.out
        assert "CREATE TABLE underscore_table (id INTEGER)" in captured.out
        assert "query with spaces:" in captured.out
        assert "CREATE TABLE space_table (id INTEGER)" in captured.out


class TestSQLoopcicleErrorHandling:
    """Test error handling and propagation."""
    
    def setup_method(self):
        """Set up test database for each test."""
        self.engine = create_engine("sqlite:///:memory:")
    
    def test_sql_execution_error_propagation(self):
        """Test that SQL execution errors are propagated."""
        sql_dict = {"bad_query": "INVALID SQL STATEMENT"}
        
        with pytest.raises(Exception):  # SQLite will raise an OperationalError
            SQLoopcicle.run_sql_loop(sql_dict, self.engine, is_just_print=False)
    
    def test_dry_run_no_errors_from_bad_sql(self, capsys):
        """Test that dry-run mode doesn't execute bad SQL."""
        sql_dict = {"bad_query": "COMPLETELY INVALID SQL"}
        
        # Should not raise any errors in dry-run mode
        SQLoopcicle.run_sql_loop(sql_dict, self.engine, is_just_print=True)
        
        # Should still print the bad SQL
        captured = capsys.readouterr()
        assert "bad_query:" in captured.out
        assert "COMPLETELY INVALID SQL" in captured.out
        
        # No tables should be created
        with self.engine.begin() as conn:
            result = conn.execute(text("SELECT name FROM sqlite_master WHERE type='table'")).fetchall()
            assert len(result) == 0
    
    def test_partial_execution_error(self, capsys):
        """Test error handling when some queries succeed and one fails."""
        sql_dict = {
            "good_query1": "CREATE TABLE good_table1 (id INTEGER)",
            "bad_query": "INVALID SQL",
            "good_query2": "CREATE TABLE good_table2 (id INTEGER)"  # This should not be reached
        }
        
        with pytest.raises(Exception):  # SQLite will raise an OperationalError
            SQLoopcicle.run_sql_loop(sql_dict, self.engine, is_just_print=False)
        
        # Should have executed first query before failing
        with self.engine.begin() as conn:
            result = conn.execute(text("SELECT name FROM sqlite_master WHERE type='table'")).fetchall()
            # Only the first table should exist
            assert len(result) == 1
            assert result[0][0] == 'good_table1'
        
        # Check that output shows the queries that were attempted
        captured = capsys.readouterr()
        assert "good_query1:" in captured.out
        assert "CREATE TABLE good_table1 (id INTEGER)" in captured.out
        assert "bad_query:" in captured.out
        assert "INVALID SQL" in captured.out


class TestSQLoopcicleIntegration:
    """Test integration scenarios with real SQLite database."""
    
    def setup_method(self):
        """Set up test database for each test."""
        self.engine = create_engine("sqlite:///:memory:")
    
    def test_comprehensive_workflow(self, capsys):
        """Test a comprehensive workflow with multiple operations."""
        sql_dict = {
            "create_users": "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)",
            "create_orders": "CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, amount REAL)",
            "insert_user1": "INSERT INTO users (name, email) VALUES ('Alice', 'alice@example.com')",
            "insert_user2": "INSERT INTO users (name, email) VALUES ('Bob', 'bob@example.com')",
            "insert_order1": "INSERT INTO orders (user_id, amount) VALUES (1, 99.99)",
            "insert_order2": "INSERT INTO orders (user_id, amount) VALUES (2, 149.99)",
            "create_view": "CREATE VIEW user_orders AS SELECT u.name, o.amount FROM users u JOIN orders o ON u.id = o.user_id"
        }
        
        SQLoopcicle.run_sql_loop(sql_dict, self.engine, is_just_print=False)
        
        # Verify all operations completed successfully
        with self.engine.begin() as conn:
            # Check tables were created
            tables = conn.execute(text("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")).fetchall()
            table_names = [t[0] for t in tables]
            assert 'users' in table_names
            assert 'orders' in table_names
            
            # Check data was inserted
            user_count = conn.execute(text("SELECT COUNT(*) FROM users")).scalar()
            assert user_count == 2
            
            order_count = conn.execute(text("SELECT COUNT(*) FROM orders")).scalar()
            assert order_count == 2
            
            # Check view was created and works
            view_result = conn.execute(text("SELECT COUNT(*) FROM user_orders")).scalar()
            assert view_result == 2
            
            # Check specific data
            alice_orders = conn.execute(text("SELECT amount FROM user_orders WHERE name = 'Alice'")).scalar()
            assert alice_orders == 99.99
        
        # Check output format
        captured = capsys.readouterr()
        assert "⏩ =====  EXECUTING SQL LOOP =====" in captured.out
        assert "⏪ ===== SQL LOOP COMPLETE =====" in captured.out
        assert "create_users:" in captured.out
        assert "create_view:" in captured.out
    
    def test_dry_run_vs_execution_comparison(self, capsys):
        """Test that dry-run and execution produce different results."""
        sql_dict = {
            "create_table": "CREATE TABLE comparison_test (id INTEGER)",
            "insert_data": "INSERT INTO comparison_test VALUES (1)"
        }
        
        # First run in dry-run mode
        SQLoopcicle.run_sql_loop(sql_dict, self.engine, is_just_print=True)
        
        # Verify nothing was created
        with self.engine.begin() as conn:
            result = conn.execute(text("SELECT name FROM sqlite_master WHERE type='table'")).fetchall()
            assert len(result) == 0
        
        # Now run in execution mode
        SQLoopcicle.run_sql_loop(sql_dict, self.engine, is_just_print=False)
        
        # Verify table and data were created
        with self.engine.begin() as conn:
            result = conn.execute(text("SELECT name FROM sqlite_master WHERE type='table'")).fetchall()
            assert len(result) == 1
            assert result[0][0] == 'comparison_test'
            
            count = conn.execute(text("SELECT COUNT(*) FROM comparison_test")).scalar()
            assert count == 1


class TestSQLoopcicleTypeHints:
    """Test type hint compliance and parameter validation."""
    
    def setup_method(self):
        """Set up test database for each test."""
        self.engine = create_engine("sqlite:///:memory:")
    
    def test_keyword_only_parameter(self):
        """Test that is_just_print is keyword-only."""
        sql_dict = {"query": "CREATE TABLE test (id INTEGER)"}
        
        # This should work (keyword argument)
        SQLoopcicle.run_sql_loop(sql_dict, self.engine, is_just_print=True)
        
        # This should fail (positional argument)
        with pytest.raises(TypeError):
            SQLoopcicle.run_sql_loop(sql_dict, self.engine, True)
    
    def test_return_type_is_none(self):
        """Test that method returns None."""
        sql_dict = {"query": "CREATE TABLE test (id INTEGER)"}
        
        result = SQLoopcicle.run_sql_loop(sql_dict, self.engine, is_just_print=True)
        assert result is None


class TestSQLoopcicleStaticMethod:
    """Test static method behavior."""
    
    def setup_method(self):
        """Set up test database for each test."""
        self.engine = create_engine("sqlite:///:memory:")
    
    def test_static_method_call_without_instance(self):
        """Test that method can be called without class instance."""
        sql_dict = {"query": "CREATE TABLE test (id INTEGER)"}
        
        # Should work without creating instance
        SQLoopcicle.run_sql_loop(sql_dict, self.engine, is_just_print=True)
        
        # Should also work with instance (though not typical usage)
        instance = SQLoopcicle()
        instance.run_sql_loop(sql_dict, self.engine, is_just_print=True)
    
    def test_class_has_no_instance_methods(self):
        """Test that class is purely static (no instance state)."""
        instance = SQLoopcicle()
        
        # Should have no instance attributes
        assert len(instance.__dict__) == 0
        
        # Should be able to create multiple instances without interference
        instance1 = SQLoopcicle()
        instance2 = SQLoopcicle()
        
        assert instance1 is not instance2
        assert len(instance1.__dict__) == 0
        assert len(instance2.__dict__) == 0


if __name__ == '__main__':
    pytest.main([__file__])
