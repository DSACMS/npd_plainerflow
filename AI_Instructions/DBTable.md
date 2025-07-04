# DBTable

DBTable merges a helpful "print the specific db_name.table_name" helper with a reflected sqlalchemy ORM.

This is a class that is part of the main plainerflow package. It should be available in the namespace after an import plainerflow statement in python.

It is acceptable for the class to inherit from SQLAlchemy's DeclarativeBase when an engine is provided, with the corresponding string overrides to ensure that it can resolve to a specific db_name.table_name when included in an f-string that will become SQL, for instance. Also, the DBTable class should function and produce the appropriate __str__ etc values, when there is no corresponding table in the database yet. And the create_child functions should work as well.

Different SQL databases have different levels of database:

- In MySQL it is database_name.table_name (Two levels)
- In PostgreSQL it is database_name.schema_name.table_name which is three levels, but you cannot actually join across databases, so schema is really highest level for SQL query purposes
- In Databricks/Spark there are three levels in a potential join statement catalog_name.database_name.table_name

If we do a taxonomy of these, then we get the simple rule:

```
Server or Project
      ↓
Catalog
      ↓
Database
      ↓
Schema
      ↓
Table or View
```

I want to create a simple python class with no dependencies that has the following features:

- First, it should use only named parameters
- The named parameters must -at least- be two of the above, so that a sensible db.table or schema.table etc etc, can be created. One is not enough. Two at the same level (i.e. table or view is not enough).
- The arguments for 'table' and 'table_name' should be treated as equivalent including database, database_name, db, db_name etc etc.
- The __str__ function and related stuff should reliably work in f-strings and other similar context, so that an object of this class can safely represent a specific table in a SQL query, and it will convert correctly to a string when needed.
- The object has a make_child, which clones accepts a suffix to append to a table name (after an underscore). create_child should be a convenience function that does the same thing.
- So that if you have an DBTable object that would resolve to this_schema.this_table and this was contained in the my_DBT object, then new_DBT = my_DBT.make_child('longer') would result in a DBTable object that would resolve to this_schema.this_table_longer
- The DBTable class should be capable of "becoming a SQL alchemy ORM" when a viable sqlalchemy engine is passed in as an argument along with the various names. Normally there is no special __str__ logic for sqlalchemy ORMs, nor is there a make_child function. Here is a python function that demonstrates how you would honor the taxonomy to create just the ORM model, as a starting point.
- The class should not support special characters other than underscore or dashes. Spaces are excluded too in the names at any level. All names should begin with letter and not numbers. When someone passes in an argument that has spaces, special characters or character lengths over 60 (which is a conservative limit)

As an example of the validity of different combinations of arguments:

- `DBTable(database='mydb', table='mytable')` ✓ (valid - different levels)
- `DBTable(table='table1', view='view1')` ✗ (invalid - same level, should throw an appropriate error)

```python
def reflect_table_model(
    *,
    table_name: str,    
    engine | None = None,
    schema_name: str | None = None,
    database_name: str | None = None,
    catalog_name: str | None = None,
    python_class_name: str | None = None,
):
    """
    Return a SQLAlchemy ORM class that is *reflected* from an existing table.

    Parameters
    ----------
    table_name : str
        The table to map (required).   
    engine : sqlalchemy.engine.Engine
        A live SQLAlchemy engine already connected to the server.
    schema_name : str, optional
        Lowest-level namespace (e.g., PostgreSQL schema, SQL Server schema).
    database_name : str, optional
        Mid-level namespace (e.g., MySQL database, Snowflake database).
    catalog_name : str, optional
        Top-level namespace (e.g., Databricks / Unity Catalog, BigQuery project).
    python_class_name : str, optional
        Override the generated Python class name.

    Returns
    -------
    type
        A new ORM class (sub-class of `DeclarativeBase`) mapped to the table.
    """

    # ------------------------------------------------------------------
    # 1.   Compose the "schema" string SQLAlchemy understands.
    #      SQLAlchemy has only one keyword argument (`schema`) for *everything*
    #      that appears to the left of the table name, so we concatenate
    #      the pieces that exist for the current dialect.
    # ------------------------------------------------------------------
    namespace_parts = []

    # Highest → lowest
    if catalog_name:
        namespace_parts.append(catalog_name)

    if database_name:
        namespace_parts.append(database_name)

    if schema_name:
        # Avoid duplicates if schema_name already supplied as database_name
        if schema_name not in namespace_parts:
            namespace_parts.append(schema_name)

    composite_schema = ".".join(namespace_parts) if namespace_parts else None

    # ------------------------------------------------------------------
    # 2.   Reflect the table
    # ------------------------------------------------------------------
    metadata_obj = MetaData()

    reflected_table = Table(
        table_name,
        metadata_obj,
        autoload_with=engine,
        schema=composite_schema,   # None means "use default search path"
    )

    # ------------------------------------------------------------------
    # 3.   Dynamically create the ORM class
    # ------------------------------------------------------------------
    default_py_class_name = f"{table_name.title().replace('_', '')}Model"
    orm_class_name = python_class_name or default_py_class_name

    orm_class = type(
        orm_class_name,
        (DeclarativeBase,),
        {"__table__": reflected_table},
    )

    return orm_class
```

## Implementation Plan

### Core Requirements Summary

1. **SQLAlchemy Integration**: The class can inherit from DeclarativeBase when an engine is provided, but must maintain string functionality for f-strings and SQL queries
2. **Validation**: Names must start with letters, contain only letters/numbers/underscores/dashes, no spaces, max 60 characters
3. **Level Validation**: Must have at least 2 parameters from different hierarchy levels
4. **Functionality**: Must work even when the table doesn't exist in the database yet

### Implementation Structure

**File**: `plainerflow/dbtable.py`

**Key Components**:

1. **Parameter Aliases Mapping**:
   ```python
   ALIASES = {
       'table': ['table', 'table_name'],
       'view': ['view', 'view_name'],
       'schema': ['schema', 'schema_name'], 
       'database': ['database', 'database_name', 'db', 'db_name'],
       'catalog': ['catalog', 'catalog_name']
   }
   ```

2. **Validation Functions**:
   - `_validate_name()`: Check character rules, length limits, starts with letter
   - `_validate_hierarchy()`: Ensure at least 2 different levels provided
   - Raise appropriate errors for invalid combinations

3. **DBTable Class**:
   - Keyword-only constructor (`*,`)
   - Store normalized hierarchy levels
   - `__str__()` method for SQL query compatibility (e.g., "schema.table")
   - `__repr__()` for debugging
   - `make_child(suffix)` and `create_child(suffix)` methods
   - `to_orm(engine)` method using the provided reflection logic

4. **Hybrid Approach**:
   - Base class functionality for string operations and child creation
   - Optional ORM conversion when engine provided
   - Maintain string behavior even in ORM mode for f-string compatibility

### Error Handling

- Clear error messages for invalid name formats
- Specific errors for same-level parameter combinations
- Validation errors for missing required parameters

### Integration

- Add to `plainerflow/__init__.py` imports
- Add to `__all__` list
- Follow existing code style patterns

### Example Usage

```python
# Basic usage
table = DBTable(database='mydb', table='users')
print(f"SELECT * FROM {table}")  # "SELECT * FROM mydb.users"

# Child creation
backup_table = table.make_child('backup')
print(backup_table)  # "mydb.users_backup"

# SQLAlchemy ORM conversion
engine = create_engine("sqlite:///test.db")
UserModel = table.to_orm(engine)  # Returns SQLAlchemy ORM class
```

The implementation will handle cases like:

- `DBTable(database='mydb', table='users')` → `"mydb.users"`
- `DBTable(catalog='cat', database='db', table='tbl')` → `"cat.db.tbl"`
- `my_table.make_child('backup')` → new DBTable with `table_backup`
