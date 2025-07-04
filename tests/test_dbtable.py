"""
Tests for DBTable class.

Tests cover parameter validation, hierarchy requirements, string representation,
child creation, and SQLAlchemy ORM integration.
"""

import pytest
import sqlite3
from unittest.mock import Mock, patch
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String
from sqlalchemy.orm import DeclarativeBase

from plainerflow.dbtable import (
    DBTable, 
    DBTableError, 
    DBTableValidationError, 
    DBTableHierarchyError
)


class TestDBTableBasicCreation:
    """Test basic DBTable creation and validation."""
    
    def test_valid_two_level_creation(self):
        """Test creating DBTable with valid two-level hierarchy."""
        table = DBTable(database='mydb', table='users')
        assert table.database == 'mydb'
        assert table.table == 'users'
        assert table.catalog is None
        assert table.schema is None
        assert table.view is None
    
    def test_valid_three_level_creation(self):
        """Test creating DBTable with valid three-level hierarchy."""
        table = DBTable(catalog='main', database='mydb', table='users')
        assert table.catalog == 'main'
        assert table.database == 'mydb'
        assert table.table == 'users'
        assert table.schema is None
        assert table.view is None
    
    def test_postgresql_style_creation(self):
        """Test PostgreSQL-style creation with database, schema, table."""
        table = DBTable(database='mydb', schema='public', table='users')
        assert table.database == 'mydb'
        assert table.schema == 'public'
        assert table.table == 'users'
        assert table.catalog is None
        assert table.view is None
    
    def test_view_instead_of_table(self):
        """Test creating DBTable with view instead of table."""
        table = DBTable(database='mydb', view='user_view')
        assert table.database == 'mydb'
        assert table.view == 'user_view'
        assert table.table is None


class TestDBTableParameterAliases:
    """Test parameter alias functionality."""
    
    def test_database_aliases(self):
        """Test all database parameter aliases work."""
        # Test database
        table1 = DBTable(database='mydb', table='users')
        assert table1.database == 'mydb'
        
        # Test database_name
        table2 = DBTable(database_name='mydb', table='users')
        assert table2.database == 'mydb'
        
        # Test db
        table3 = DBTable(db='mydb', table='users')
        assert table3.database == 'mydb'
        
        # Test db_name
        table4 = DBTable(db_name='mydb', table='users')
        assert table4.database == 'mydb'
    
    def test_table_aliases(self):
        """Test table parameter aliases work."""
        # Test table
        table1 = DBTable(database='mydb', table='users')
        assert table1.table == 'users'
        
        # Test table_name
        table2 = DBTable(database='mydb', table_name='users')
        assert table2.table == 'users'
    
    def test_schema_aliases(self):
        """Test schema parameter aliases work."""
        # Test schema
        table1 = DBTable(database='mydb', schema='public', table='users')
        assert table1.schema == 'public'
        
        # Test schema_name
        table2 = DBTable(database='mydb', schema_name='public', table='users')
        assert table2.schema == 'public'
    
    def test_catalog_aliases(self):
        """Test catalog parameter aliases work."""
        # Test catalog
        table1 = DBTable(catalog='main', database='mydb', table='users')
        assert table1.catalog == 'main'
        
        # Test catalog_name
        table2 = DBTable(catalog_name='main', database='mydb', table='users')
        assert table2.catalog == 'main'
    
    def test_view_aliases(self):
        """Test view parameter aliases work."""
        # Test view
        table1 = DBTable(database='mydb', view='user_view')
        assert table1.view == 'user_view'
        
        # Test view_name
        table2 = DBTable(database='mydb', view_name='user_view')
        assert table2.view == 'user_view'


class TestDBTableValidation:
    """Test parameter validation."""
    
    def test_empty_name_validation(self):
        """Test that empty names are rejected."""
        with pytest.raises(DBTableValidationError, match="database name cannot be empty"):
            DBTable(database='', table='users')
    
    def test_name_length_validation(self):
        """Test that names over 60 characters are rejected."""
        long_name = 'a' * 61
        with pytest.raises(DBTableValidationError, match="exceeds 60 character limit"):
            DBTable(database=long_name, table='users')
    
    def test_name_must_start_with_letter(self):
        """Test that names must start with a letter."""
        with pytest.raises(DBTableValidationError, match="must start with a letter"):
            DBTable(database='1database', table='users')
        
        with pytest.raises(DBTableValidationError, match="must start with a letter"):
            DBTable(database='_database', table='users')
    
    def test_invalid_characters_validation(self):
        """Test that invalid characters are rejected."""
        # Test spaces
        with pytest.raises(DBTableValidationError, match="contains invalid characters"):
            DBTable(database='my database', table='users')
        
        # Test special characters
        with pytest.raises(DBTableValidationError, match="contains invalid characters"):
            DBTable(database='my@database', table='users')
        
        # Test dots
        with pytest.raises(DBTableValidationError, match="contains invalid characters"):
            DBTable(database='my.database', table='users')
    
    def test_valid_characters(self):
        """Test that valid characters are accepted."""
        # Letters, numbers, underscores, dashes
        table = DBTable(database='my_data-base123', table='user_table-v2')
        assert table.database == 'my_data-base123'
        assert table.table == 'user_table-v2'
    
    def test_unknown_parameter(self):
        """Test that unknown parameters are rejected."""
        with pytest.raises(DBTableValidationError, match="Unknown parameter: invalid_param"):
            DBTable(database='mydb', table='users', invalid_param='value')
    
    def test_duplicate_level_parameters(self):
        """Test that multiple parameters for same level are rejected."""
        with pytest.raises(DBTableValidationError, match="Multiple parameters provided for database level"):
            DBTable(database='mydb', db='mydb2', table='users')


class TestDBTableHierarchyValidation:
    """Test hierarchy validation requirements."""
    
    def test_no_parameters_error(self):
        """Test that no parameters raises error."""
        with pytest.raises(DBTableHierarchyError, match="At least two hierarchy level parameters are required"):
            DBTable()
    
    def test_single_parameter_error(self):
        """Test that single parameter raises error."""
        with pytest.raises(DBTableHierarchyError, match="At least 2 different hierarchy levels required"):
            DBTable(table='users')
    
    def test_same_level_parameters_error(self):
        """Test that same-level parameters raise error."""
        with pytest.raises(DBTableHierarchyError, match="Cannot specify multiple parameters from the same level"):
            DBTable(table='users', view='user_view')
    
    def test_valid_different_levels(self):
        """Test that different levels are accepted."""
        # These should all work
        DBTable(database='db', table='tbl')
        DBTable(catalog='cat', database='db', table='tbl')
        DBTable(database='db', schema='sch', table='tbl')
        DBTable(catalog='cat', database='db', schema='sch', table='tbl')


class TestDBTableStringRepresentation:
    """Test string representation methods."""
    
    def test_str_two_levels(self):
        """Test __str__ with two levels."""
        table = DBTable(database='mydb', table='users')
        assert str(table) == 'mydb.users'
    
    def test_str_three_levels(self):
        """Test __str__ with three levels."""
        table = DBTable(catalog='main', database='mydb', table='users')
        assert str(table) == 'main.mydb.users'
    
    def test_str_postgresql_style(self):
        """Test __str__ with PostgreSQL-style hierarchy."""
        table = DBTable(database='mydb', schema='public', table='users')
        assert str(table) == 'mydb.public.users'
    
    def test_str_full_hierarchy(self):
        """Test __str__ with full hierarchy."""
        table = DBTable(catalog='main', database='mydb', schema='public', table='users')
        assert str(table) == 'main.mydb.public.users'
    
    def test_str_with_view(self):
        """Test __str__ with view instead of table."""
        table = DBTable(database='mydb', view='user_view')
        assert str(table) == 'mydb.user_view'
    
    def test_repr(self):
        """Test __repr__ method."""
        table = DBTable(database='mydb', table='users')
        assert repr(table) == "DBTable(database='mydb', table='users')"
    
    def test_repr_full_hierarchy(self):
        """Test __repr__ with full hierarchy."""
        table = DBTable(catalog='main', database='mydb', schema='public', table='users')
        expected = "DBTable(catalog='main', database='mydb', schema='public', table='users')"
        assert repr(table) == expected
    
    def test_f_string_usage(self):
        """Test that DBTable works correctly in f-strings."""
        table = DBTable(database='mydb', table='users')
        query = f"SELECT * FROM {table}"
        assert query == "SELECT * FROM mydb.users"


class TestDBTableChildCreation:
    """Test child creation functionality."""
    
    def test_make_child_with_table(self):
        """Test make_child with table name."""
        parent = DBTable(database='mydb', table='users')
        child = parent.make_child('backup')
        
        assert child.database == 'mydb'
        assert child.table == 'users_backup'
        assert str(child) == 'mydb.users_backup'
    
    def test_make_child_with_view(self):
        """Test make_child with view name."""
        parent = DBTable(database='mydb', view='user_view')
        child = parent.make_child('temp')
        
        assert child.database == 'mydb'
        assert child.view == 'user_view_temp'
        assert str(child) == 'mydb.user_view_temp'
    
    def test_make_child_preserves_hierarchy(self):
        """Test that make_child preserves full hierarchy."""
        parent = DBTable(catalog='main', database='mydb', schema='public', table='users')
        child = parent.make_child('archive')
        
        assert child.catalog == 'main'
        assert child.database == 'mydb'
        assert child.schema == 'public'
        assert child.table == 'users_archive'
        assert str(child) == 'main.mydb.public.users_archive'
    
    def test_create_child_alias(self):
        """Test that create_child works as alias for make_child."""
        parent = DBTable(database='mydb', table='users')
        child = parent.create_child('backup')
        
        assert child.database == 'mydb'
        assert child.table == 'users_backup'
        assert str(child) == 'mydb.users_backup'
    
    def test_make_child_without_table_or_view(self):
        """Test that make_child fails without table or view."""
        # This is a bit artificial since we require at least 2 levels,
        # but let's test the error handling
        with pytest.raises(DBTableValidationError, match="Cannot create child: no table or view name defined"):
            # We can't actually create this scenario with current validation,
            # so we'll mock it
            table = DBTable(database='mydb', table='users')
            table.table = None  # Artificially remove table
            table.make_child('backup')
    
    def test_make_child_suffix_validation(self):
        """Test that make_child validates suffix."""
        parent = DBTable(database='mydb', table='users')
        
        # Invalid suffix should fail
        with pytest.raises(DBTableValidationError, match="suffix name"):
            parent.make_child('123invalid')
        
        with pytest.raises(DBTableValidationError, match="suffix name"):
            parent.make_child('invalid suffix')


class TestDBTableSQLAlchemyIntegration:
    """Test SQLAlchemy ORM integration."""
    
    def setup_method(self):
        """Set up test database for SQLAlchemy tests."""
        # Create in-memory SQLite database
        self.engine = create_engine("sqlite:///:memory:")
        
        # Create a test table
        metadata = MetaData()
        self.test_table = Table(
            'users',
            metadata,
            Column('id', Integer, primary_key=True),
            Column('name', String(50)),
            Column('email', String(100))
        )
        metadata.create_all(self.engine)
    
    def test_to_orm_basic(self):
        """Test basic ORM creation."""
        table = DBTable(database='testdb', table='users')
        
        # Mock the reflection since we can't easily test with real schema
        with patch('plainerflow.dbtable.Table') as mock_table, \
             patch('plainerflow.dbtable.MetaData') as mock_metadata:
            
            mock_reflected_table = Mock()
            mock_table.return_value = mock_reflected_table
            
            orm_class = table.to_orm(self.engine)
            
            # Verify the ORM class was created
            assert orm_class.__name__ == 'UsersModel'
            assert hasattr(orm_class, '__table__')
            
            # Verify Table was called correctly
            mock_table.assert_called_once_with(
                'users',
                mock_metadata.return_value,
                autoload_with=self.engine,
                schema='testdb'
            )
    
    def test_to_orm_with_schema(self):
        """Test ORM creation with schema."""
        table = DBTable(database='mydb', schema='public', table='users')
        
        with patch('plainerflow.dbtable.Table') as mock_table, \
             patch('plainerflow.dbtable.MetaData') as mock_metadata:
            
            mock_reflected_table = Mock()
            mock_table.return_value = mock_reflected_table
            
            orm_class = table.to_orm(self.engine)
            
            # Verify Table was called with correct schema
            mock_table.assert_called_once_with(
                'users',
                mock_metadata.return_value,
                autoload_with=self.engine,
                schema='mydb.public'
            )
    
    def test_to_orm_with_full_hierarchy(self):
        """Test ORM creation with full hierarchy."""
        table = DBTable(catalog='main', database='mydb', schema='public', table='users')
        
        with patch('plainerflow.dbtable.Table') as mock_table, \
             patch('plainerflow.dbtable.MetaData') as mock_metadata:
            
            mock_reflected_table = Mock()
            mock_table.return_value = mock_reflected_table
            
            orm_class = table.to_orm(self.engine)
            
            # Verify Table was called with correct composite schema
            mock_table.assert_called_once_with(
                'users',
                mock_metadata.return_value,
                autoload_with=self.engine,
                schema='main.mydb.public'
            )
    
    def test_to_orm_custom_class_name(self):
        """Test ORM creation with custom class name."""
        table = DBTable(database='testdb', table='users')
        
        with patch('plainerflow.dbtable.Table') as mock_table, \
             patch('plainerflow.dbtable.MetaData') as mock_metadata:
            
            mock_reflected_table = Mock()
            mock_table.return_value = mock_reflected_table
            
            orm_class = table.to_orm(self.engine, python_class_name='CustomUser')
            
            assert orm_class.__name__ == 'CustomUser'
    
    def test_to_orm_with_view(self):
        """Test ORM creation with view."""
        table = DBTable(database='mydb', view='user_view')
        
        with patch('plainerflow.dbtable.Table') as mock_table, \
             patch('plainerflow.dbtable.MetaData') as mock_metadata:
            
            mock_reflected_table = Mock()
            mock_table.return_value = mock_reflected_table
            
            orm_class = table.to_orm(self.engine)
            
            # Should use view name
            mock_table.assert_called_once_with(
                'user_view',
                mock_metadata.return_value,
                autoload_with=self.engine,
                schema='mydb'
            )
            
            assert orm_class.__name__ == 'UserViewModel'
    
    def test_to_orm_without_table_or_view(self):
        """Test that to_orm fails without table or view."""
        # Again, artificial scenario for error testing
        with pytest.raises(DBTableValidationError, match="Cannot create ORM: no table or view name defined"):
            table = DBTable(database='mydb', table='users')
            table.table = None  # Artificially remove table
            table.to_orm(self.engine)


class TestDBTableEdgeCases:
    """Test edge cases and integration scenarios."""
    
    def test_complex_names(self):
        """Test complex but valid names."""
        table = DBTable(
            catalog='my-catalog_v2',
            database='test_db-prod',
            schema='user_schema-v1',
            table='user_events_2024'
        )
        
        expected = 'my-catalog_v2.test_db-prod.user_schema-v1.user_events_2024'
        assert str(table) == expected
    
    def test_case_sensitivity(self):
        """Test that names preserve case."""
        table = DBTable(database='MyDatabase', table='UserTable')
        assert str(table) == 'MyDatabase.UserTable'
    
    def test_numeric_in_names(self):
        """Test that numbers in names work (but not at start)."""
        table = DBTable(database='db2024', table='users_v2')
        assert str(table) == 'db2024.users_v2'
    
    def test_child_creation_chain(self):
        """Test creating children of children."""
        parent = DBTable(database='mydb', table='users')
        child1 = parent.make_child('backup')
        child2 = child1.make_child('temp')
        
        assert str(child2) == 'mydb.users_backup_temp'
    
    def test_maximum_length_names(self):
        """Test names at maximum allowed length."""
        max_name = 'a' * 60  # Exactly 60 characters
        table = DBTable(database=max_name, table='users')
        assert table.database == max_name


if __name__ == '__main__':
    pytest.main([__file__])
