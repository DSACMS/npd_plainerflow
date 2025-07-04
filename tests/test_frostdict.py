"""
Tests for FrostDict class.

Tests cover frozen key behavior, nested mutability, hashability,
dictionary-like operations, and integration with SQLoopcicle.
"""

import pytest
from plainerflow.frostdict import FrostDict, FrozenKeyError
from plainerflow.sqloopcicle import SQLoopcicle
from sqlalchemy import create_engine


class TestFrostDictBasicFunctionality:
    """Test basic FrostDict functionality."""
    
    def test_initialization_empty(self):
        """Test creating empty FrostDict."""
        fd = FrostDict()
        assert len(fd) == 0
        assert list(fd.keys()) == []
        assert list(fd.items()) == []
    
    def test_initialization_with_data(self):
        """Test creating FrostDict with initial data."""
        initial_data = {'key1': 'value1', 'key2': 'value2'}
        fd = FrostDict(initial_data)
        
        assert len(fd) == 2
        assert fd['key1'] == 'value1'
        assert fd['key2'] == 'value2'
        assert 'key1' in fd
        assert 'key2' in fd
    
    def test_getitem_access(self):
        """Test dictionary-style access with []."""
        fd = FrostDict({'test_key': 'test_value'})
        assert fd['test_key'] == 'test_value'
    
    def test_getitem_keyerror(self):
        """Test KeyError on missing key."""
        fd = FrostDict()
        with pytest.raises(KeyError):
            _ = fd['nonexistent_key']
    
    def test_contains_operator(self):
        """Test 'in' operator."""
        fd = FrostDict({'existing': 'value'})
        assert 'existing' in fd
        assert 'nonexistent' not in fd
    
    def test_len_function(self):
        """Test len() function."""
        fd = FrostDict()
        assert len(fd) == 0
        
        fd['key1'] = 'value1'
        assert len(fd) == 1
        
        fd['key2'] = 'value2'
        assert len(fd) == 2
    
    def test_iteration_over_keys(self):
        """Test iteration over keys."""
        data = {'a': 1, 'b': 2, 'c': 3}
        fd = FrostDict(data)
        
        keys = list(fd)
        assert set(keys) == set(data.keys())
        
        # Test that iteration works multiple times
        keys2 = list(fd)
        assert keys == keys2
    
    def test_keys_method(self):
        """Test .keys() method."""
        data = {'x': 10, 'y': 20}
        fd = FrostDict(data)
        
        keys = fd.keys()
        assert set(keys) == set(data.keys())
        assert isinstance(keys, type(data.keys()))  # Should be same type as dict.keys()
    
    def test_items_method(self):
        """Test .items() method."""
        data = {'name': 'Alice', 'age': 30}
        fd = FrostDict(data)
        
        items = fd.items()
        assert set(items) == set(data.items())
        assert isinstance(items, type(data.items()))  # Should be same type as dict.items()
    
    def test_values_method(self):
        """Test .values() method."""
        data = {'a': 'apple', 'b': 'banana'}
        fd = FrostDict(data)
        
        values = fd.values()
        assert set(values) == set(data.values())
        assert isinstance(values, type(data.values()))  # Should be same type as dict.values()
    
    def test_get_method(self):
        """Test .get() method with default values."""
        fd = FrostDict({'existing': 'value'})
        
        assert fd.get('existing') == 'value'
        assert fd.get('nonexistent') is None
        assert fd.get('nonexistent', 'default') == 'default'


class TestFrostDictFrozenBehavior:
    """Test the frozen key behavior - core functionality."""
    
    def test_first_insertion_allowed(self):
        """Test that adding new keys is allowed."""
        fd = FrostDict()
        
        # Should be able to add new keys
        fd['new_key'] = 'new_value'
        assert fd['new_key'] == 'new_value'
        
        fd['another_key'] = 'another_value'
        assert fd['another_key'] == 'another_value'
        assert len(fd) == 2
    
    def test_reassignment_raises_frozen_key_error(self):
        """Test that reassigning existing keys raises FrozenKeyError."""
        fd = FrostDict({'existing_key': 'original_value'})
        
        # Should raise FrozenKeyError when trying to reassign
        with pytest.raises(FrozenKeyError) as exc_info:
            fd['existing_key'] = 'new_value'
        
        assert "Cannot reassign existing key 'existing_key' in FrostDict" in str(exc_info.value)
        
        # Original value should be unchanged
        assert fd['existing_key'] == 'original_value'
    
    def test_reassignment_after_initial_assignment(self):
        """Test that keys become frozen after first assignment."""
        fd = FrostDict()
        
        # First assignment should work
        fd['key'] = 'first_value'
        assert fd['key'] == 'first_value'
        
        # Second assignment should fail
        with pytest.raises(FrozenKeyError):
            fd['key'] = 'second_value'
        
        # Value should remain unchanged
        assert fd['key'] == 'first_value'
    
    def test_multiple_keys_frozen_independently(self):
        """Test that each key is frozen independently."""
        fd = FrostDict()
        
        # Add multiple keys
        fd['key1'] = 'value1'
        fd['key2'] = 'value2'
        
        # Both should be frozen
        with pytest.raises(FrozenKeyError):
            fd['key1'] = 'new_value1'
        
        with pytest.raises(FrozenKeyError):
            fd['key2'] = 'new_value2'
        
        # But we can still add new keys
        fd['key3'] = 'value3'
        assert fd['key3'] == 'value3'
    
    def test_frozen_key_error_custom_exception(self):
        """Test that FrozenKeyError is a proper custom exception."""
        fd = FrostDict({'key': 'value'})
        
        with pytest.raises(FrozenKeyError) as exc_info:
            fd['key'] = 'new_value'
        
        # Should be instance of Exception
        assert isinstance(exc_info.value, Exception)
        
        # Should have meaningful message
        assert 'key' in str(exc_info.value)
        assert 'FrostDict' in str(exc_info.value)


class TestFrostDictNestedMutability:
    """Test that nested values remain mutable."""
    
    def test_nested_dict_mutability(self):
        """Test that nested dictionaries can be modified."""
        nested_dict = {'setting': 'original_value'}
        fd = FrostDict({'config': nested_dict})
        
        # Should be able to modify nested dict
        fd['config']['setting'] = 'new_value'
        assert fd['config']['setting'] == 'new_value'
        
        # Should be able to add to nested dict
        fd['config']['new_setting'] = 'another_value'
        assert fd['config']['new_setting'] == 'another_value'
        
        # But top-level key should still be frozen
        with pytest.raises(FrozenKeyError):
            fd['config'] = {'completely': 'different'}
    
    def test_nested_list_mutability(self):
        """Test that nested lists can be modified."""
        nested_list = ['item1', 'item2']
        fd = FrostDict({'items': nested_list})
        
        # Should be able to modify nested list
        fd['items'].append('item3')
        assert 'item3' in fd['items']
        
        fd['items'][0] = 'modified_item1'
        assert fd['items'][0] == 'modified_item1'
        
        # But top-level key should still be frozen
        with pytest.raises(FrozenKeyError):
            fd['items'] = ['completely', 'different', 'list']
    
    def test_deeply_nested_mutability(self):
        """Test mutability in deeply nested structures."""
        deep_structure = {
            'level1': {
                'level2': {
                    'level3': ['item1', 'item2']
                }
            }
        }
        fd = FrostDict({'deep': deep_structure})
        
        # Should be able to modify deeply nested values
        fd['deep']['level1']['level2']['level3'].append('item3')
        assert len(fd['deep']['level1']['level2']['level3']) == 3
        
        fd['deep']['level1']['level2']['new_key'] = 'new_value'
        assert fd['deep']['level1']['level2']['new_key'] == 'new_value'
        
        # But top-level should still be frozen
        with pytest.raises(FrozenKeyError):
            fd['deep'] = 'simple_value'


class TestFrostDictValueTypes:
    """Test support for different value types."""
    
    def test_string_values(self):
        """Test string values including f-strings."""
        schema_name = 'test_schema'
        fd = FrostDict()
        
        fd['simple_string'] = 'Hello World'
        fd['f_string_sql'] = f"CREATE SCHEMA IF NOT EXISTS {schema_name}"
        
        assert fd['simple_string'] == 'Hello World'
        assert fd['f_string_sql'] == 'CREATE SCHEMA IF NOT EXISTS test_schema'
    
    def test_dict_values(self):
        """Test nested dictionary values."""
        fd = FrostDict()
        
        fd['config'] = {'host': 'localhost', 'port': 5432}
        assert fd['config']['host'] == 'localhost'
        assert fd['config']['port'] == 5432
    
    def test_list_values(self):
        """Test list values."""
        fd = FrostDict()
        
        fd['items'] = ['item1', 'item2', 'item3']
        assert len(fd['items']) == 3
        assert fd['items'][0] == 'item1'
    
    def test_arbitrary_object_values(self):
        """Test arbitrary Python objects as values."""
        class CustomObject:
            def __init__(self, value):
                self.value = value
        
        fd = FrostDict()
        obj = CustomObject('test_value')
        
        fd['custom_object'] = obj
        assert fd['custom_object'].value == 'test_value'
        
        # Should be able to modify object attributes
        fd['custom_object'].value = 'modified_value'
        assert fd['custom_object'].value == 'modified_value'
    
    def test_mixed_value_types(self):
        """Test mixing different value types."""
        fd = FrostDict({
            'string': 'text',
            'number': 42,
            'list': [1, 2, 3],
            'dict': {'nested': 'value'},
            'boolean': True,
            'none_value': None
        })
        
        assert fd['string'] == 'text'
        assert fd['number'] == 42
        assert fd['list'] == [1, 2, 3]
        assert fd['dict']['nested'] == 'value'
        assert fd['boolean'] is True
        assert fd['none_value'] is None


class TestFrostDictHashability:
    """Test hashability requirements."""
    
    def test_hashable_with_hashable_values(self):
        """Test that FrostDict is hashable when all values are hashable."""
        fd = FrostDict({
            'string': 'value',
            'number': 42,
            'tuple': (1, 2, 3),
            'boolean': True,
            'none_value': None
        })
        
        # Should be able to hash
        hash_value = hash(fd)
        assert isinstance(hash_value, int)
        
        # Should be able to use as dict key
        container = {fd: 'stored_value'}
        assert container[fd] == 'stored_value'
        
        # Should be able to use in set
        fd_set = {fd}
        assert fd in fd_set
    
    def test_not_hashable_with_unhashable_values(self):
        """Test that FrostDict is not hashable when containing unhashable values."""
        fd = FrostDict({
            'list': [1, 2, 3],  # Lists are not hashable
            'string': 'value'
        })
        
        with pytest.raises(TypeError) as exc_info:
            hash(fd)
        
        assert 'not hashable' in str(exc_info.value).lower()
        assert 'unhashable values' in str(exc_info.value)
    
    def test_hash_consistency(self):
        """Test that hash is consistent for same content."""
        fd1 = FrostDict({'a': 1, 'b': 2})
        fd2 = FrostDict({'a': 1, 'b': 2})
        
        assert hash(fd1) == hash(fd2)
        
        # Different order should still hash the same
        fd3 = FrostDict({'b': 2, 'a': 1})
        assert hash(fd1) == hash(fd3)
    
    def test_hash_different_for_different_content(self):
        """Test that different content produces different hashes."""
        fd1 = FrostDict({'a': 1})
        fd2 = FrostDict({'a': 2})
        fd3 = FrostDict({'b': 1})
        
        # These should likely have different hashes (not guaranteed, but very likely)
        assert hash(fd1) != hash(fd2)
        assert hash(fd1) != hash(fd3)


class TestFrostDictStringRepresentation:
    """Test string representation requirements."""
    
    def test_repr_format(self):
        """Test __repr__ format."""
        fd = FrostDict({'key1': 'value1', 'key2': 'value2'})
        repr_str = repr(fd)
        
        assert repr_str.startswith('FrostDict(')
        assert repr_str.endswith(')')
        assert 'key1' in repr_str
        assert 'value1' in repr_str
        assert 'key2' in repr_str
        assert 'value2' in repr_str
    
    def test_str_format(self):
        """Test __str__ format (should be same as __repr__)."""
        fd = FrostDict({'test': 'value'})
        assert str(fd) == repr(fd)
    
    def test_repr_empty_dict(self):
        """Test repr of empty FrostDict."""
        fd = FrostDict()
        assert repr(fd) == 'FrostDict({})'
    
    def test_repr_single_item(self):
        """Test repr with single item."""
        fd = FrostDict({'single': 'item'})
        repr_str = repr(fd)
        
        assert 'FrostDict(' in repr_str
        assert "'single': 'item'" in repr_str
    
    def test_repr_complex_values(self):
        """Test repr with complex nested values."""
        fd = FrostDict({
            'nested': {'inner': 'value'},
            'list': [1, 2, 3]
        })
        repr_str = repr(fd)
        
        assert 'FrostDict(' in repr_str
        assert 'nested' in repr_str
        assert 'inner' in repr_str
        assert 'list' in repr_str


class TestFrostDictEquality:
    """Test equality comparison."""
    
    def test_equality_with_same_frostdict(self):
        """Test equality between FrostDict instances."""
        fd1 = FrostDict({'a': 1, 'b': 2})
        fd2 = FrostDict({'a': 1, 'b': 2})
        
        assert fd1 == fd2
        assert fd2 == fd1
    
    def test_equality_with_regular_dict(self):
        """Test equality with regular dict."""
        fd = FrostDict({'a': 1, 'b': 2})
        regular_dict = {'a': 1, 'b': 2}
        
        assert fd == regular_dict
        assert regular_dict == fd
    
    def test_inequality_different_content(self):
        """Test inequality with different content."""
        fd1 = FrostDict({'a': 1})
        fd2 = FrostDict({'a': 2})
        fd3 = FrostDict({'b': 1})
        
        assert fd1 != fd2
        assert fd1 != fd3
        assert fd2 != fd3
    
    def test_inequality_with_non_dict_types(self):
        """Test inequality with non-dict types."""
        fd = FrostDict({'a': 1})
        
        assert fd != 'string'
        assert fd != 42
        assert fd != [1, 2, 3]
        assert fd != None
    
    def test_equality_empty_dicts(self):
        """Test equality of empty dicts."""
        fd1 = FrostDict()
        fd2 = FrostDict()
        empty_dict = {}
        
        assert fd1 == fd2
        assert fd1 == empty_dict
        assert empty_dict == fd1


class TestFrostDictUsagePatterns:
    """Test common usage patterns from the specification."""
    
    def test_module_load_time_definition(self):
        """Test defining FrostDict at module load time."""
        schema_name = 'test_schema'
        
        # This simulates module-level definition
        QUERIES = FrostDict({
            "create_schema": f"CREATE SCHEMA IF NOT EXISTS {schema_name}",
            "book_by_id": "SELECT * FROM books WHERE id = :book_id",
            "all_authors": "SELECT * FROM authors",
        })
        
        assert QUERIES["create_schema"] == "CREATE SCHEMA IF NOT EXISTS test_schema"
        assert QUERIES["book_by_id"] == "SELECT * FROM books WHERE id = :book_id"
        assert QUERIES["all_authors"] == "SELECT * FROM authors"
        
        # Should not be able to reassign
        with pytest.raises(FrozenKeyError):
            QUERIES["create_schema"] = "different query"
    
    def test_incremental_building_pattern(self):
        """Test building FrostDict incrementally."""
        sql = FrostDict()
        
        # Should be able to add keys incrementally
        sql['create_table'] = "CREATE TABLE users (id INTEGER, name TEXT)"
        sql['insert_user'] = "INSERT INTO users (name) VALUES (:name)"
        sql['select_all'] = "SELECT * FROM users"
        
        assert len(sql) == 3
        assert 'create_table' in sql
        assert 'insert_user' in sql
        assert 'select_all' in sql
        
        # But not reassign existing ones
        with pytest.raises(FrozenKeyError):
            sql['create_table'] = "CREATE TABLE different_table (id INTEGER)"
    
    def test_sql_template_usage(self):
        """Test usage with SQL templates and f-strings."""
        table_name = 'users'
        schema_name = 'public'
        
        sql_templates = FrostDict({
            'create_table': f"CREATE TABLE {schema_name}.{table_name} (id SERIAL PRIMARY KEY, name VARCHAR(100))",
            'drop_table': f"DROP TABLE IF EXISTS {schema_name}.{table_name}",
            'select_all': f"SELECT * FROM {schema_name}.{table_name}",
            'count_rows': f"SELECT COUNT(*) FROM {schema_name}.{table_name}"
        })
        
        assert 'public.users' in sql_templates['create_table']
        assert 'DROP TABLE IF EXISTS public.users' == sql_templates['drop_table']
        assert 'SELECT * FROM public.users' == sql_templates['select_all']
        assert 'SELECT COUNT(*) FROM public.users' == sql_templates['count_rows']


class TestFrostDictSQLoopcicleIntegration:
    """Test integration with SQLoopcicle class."""
    
    def setup_method(self):
        """Set up test database for each test."""
        self.engine = create_engine("sqlite:///:memory:")
    
    def test_frostdict_with_sqloopcicle_dry_run(self, capsys):
        """Test using FrostDict with SQLoopcicle in dry-run mode."""
        sql_queries = FrostDict({
            "create_users": "CREATE TABLE users (id INTEGER, name TEXT)",
            "create_orders": "CREATE TABLE orders (id INTEGER, user_id INTEGER, amount REAL)",
            "insert_sample": "INSERT INTO users (name) VALUES ('Test User')"
        })
        
        SQLoopcicle.run_sql_loop(sql_queries, self.engine, is_just_print=True)
        
        # Check output
        captured = capsys.readouterr()
        assert "⏩ =====  DRY-RUN MODE – NO SQL WILL BE EXECUTED =====" in captured.out
        assert "create_users:" in captured.out
        assert "CREATE TABLE users (id INTEGER, name TEXT)" in captured.out
        assert "create_orders:" in captured.out
        assert "CREATE TABLE orders (id INTEGER, user_id INTEGER, amount REAL)" in captured.out
        assert "insert_sample:" in captured.out
        assert "INSERT INTO users (name) VALUES ('Test User')" in captured.out
        assert "🟡 ===== I AM NOT RUNNING SQL =====" in captured.out
    
    def test_frostdict_with_sqloopcicle_execution(self, capsys):
        """Test using FrostDict with SQLoopcicle in execution mode."""
        sql_queries = FrostDict({
            "create_table": "CREATE TABLE test_table (id INTEGER PRIMARY KEY, data TEXT)",
            "insert_data": "INSERT INTO test_table (data) VALUES ('test_data')"
        })
        
        SQLoopcicle.run_sql_loop(sql_queries, self.engine, is_just_print=False)
        
        # Verify SQL was executed
        from sqlalchemy import text
        with self.engine.begin() as conn:
            # Check table exists
            result = conn.execute(text("SELECT name FROM sqlite_master WHERE type='table'")).fetchall()
            assert len(result) == 1
            assert result[0][0] == 'test_table'
            
            # Check data was inserted
            result = conn.execute(text("SELECT data FROM test_table")).fetchall()
            assert len(result) == 1
            assert result[0][0] == 'test_data'
        
        # Check output
        captured = capsys.readouterr()
        assert "⏩ =====  EXECUTING SQL LOOP =====" in captured.out
        assert "⏪ ===== SQL LOOP COMPLETE =====" in captured.out
    
    def test_frostdict_prevents_accidental_sql_modification(self):
        """Test that FrostDict prevents accidental modification of SQL queries."""
        sql_queries = FrostDict({
            "important_query": "SELECT * FROM critical_table WHERE status = 'active'"
        })
        
        # This should work fine with SQLoopcicle
        SQLoopcicle.run_sql_loop(sql_queries, self.engine, is_just_print=True)
        
        # But we can't accidentally modify the query later
        with pytest.raises(FrozenKeyError):
            sql_queries["important_query"] = "DROP TABLE critical_table"  # Oops!
        
        # Original query should be unchanged
        assert sql_queries["important_query"] == "SELECT * FROM critical_table WHERE status = 'active'"
    
    def test_frostdict_allows_adding_new_queries(self):
        """Test that new queries can be added to FrostDict for SQLoopcicle."""
        sql_queries = FrostDict({
            "initial_query": "CREATE TABLE initial (id INTEGER)"
        })
        
        # Should be able to add new queries
        sql_queries["additional_query"] = "CREATE TABLE additional (id INTEGER)"
        
        # Both should work with SQLoopcicle
        SQLoopcicle.run_sql_loop(sql_queries, self.engine, is_just_print=True)
        
        # But can't modify existing ones
        with pytest.raises(FrozenKeyError):
            sql_queries["initial_query"] = "CREATE TABLE modified (id INTEGER)"


class TestFrostDictEdgeCases:
    """Test edge cases and error conditions."""
    
    def test_none_as_initial_data(self):
        """Test initialization with None."""
        fd = FrostDict(None)
        assert len(fd) == 0
        assert list(fd.keys()) == []
    
    def test_empty_string_keys(self):
        """Test with empty string keys."""
        fd = FrostDict({'': 'empty_key_value'})
        assert fd[''] == 'empty_key_value'
        
        # Should still be frozen
        with pytest.raises(FrozenKeyError):
            fd[''] = 'new_value'
    
    def test_numeric_string_keys(self):
        """Test with numeric string keys."""
        fd = FrostDict({'123': 'numeric_string_key'})
        assert fd['123'] == 'numeric_string_key'
        
        with pytest.raises(FrozenKeyError):
            fd['123'] = 'modified'
    
    def test_special_character_keys(self):
        """Test with special characters in keys."""
        special_keys = {
            'key-with-dashes': 'value1',
            'key_with_underscores': 'value2',
            'key with spaces': 'value3',
            'key.with.dots': 'value4',
            'key/with/slashes': 'value5'
        }
        
        fd = FrostDict(special_keys)
        
        for key, value in special_keys.items():
            assert fd[key] == value
            
            # All should be frozen
            with pytest.raises(FrozenKeyError):
                fd[key] = 'modified'
    
    def test_very_long_keys_and_values(self):
        """Test with very long keys and values."""
        long_key = 'a' * 1000
        long_value = 'b' * 1000
        
        fd = FrostDict({long_key: long_value})
        assert fd[long_key] == long_value
        
        with pytest.raises(FrozenKeyError):
            fd[long_key] = 'different_value'
    
    def test_unicode_keys_and_values(self):
        """Test with Unicode keys and values."""
        unicode_data = {
            'café': 'coffee',
            '数据': 'data',
            '🔑': 'key_emoji',
            'Ñoño': 'spanish'
        }
        
        fd = FrostDict(unicode_data)
        
        for key, value in unicode_data.items():
            assert fd[key] == value
            
            with pytest.raises(FrozenKeyError):
                fd[key] = 'modified'


if __name__ == '__main__':
    pytest.main([__file__])
