# ConfigNoir & EngineFetcher: Advanced Configuration and Connection Management

`ConfigNoir` and `EngineFetcher` are a pair of utilities that provide a robust and flexible system for managing database configurations and connections in PlainerFlow. They are designed to separate the process of *finding* credentials from the process of *using* them to create a database engine.

## Overview

- **`ConfigNoir`**: Its primary role is to **detect and load** configuration settings from a variety of sources. It intelligently searches the environment in a prioritized order and returns a standardized `Dynaconf` settings object.
- **`EngineFetcher`**: Its sole responsibility is to take a `Dynaconf` settings object and **create a SQLAlchemy engine**.

This separation of concerns makes the system more modular and easier to test and maintain.

## `ConfigNoir`: Configuration Detection

`ConfigNoir` finds credentials in the following priority order:

1. **SQLite Override**: If a specific `sqlite_db_file` path is provided, it will be used immediately.
2. **Spark Session**: Detects an active Spark/Databricks session and extracts the JDBC connection URL.
3. **Google Colab**: In a Google Colab environment, it can read credentials from a specified Google Sheet.
4. **`.env` File**: Searches for a standard `.env` file in the project directory.
5. **Fallback**: If no other sources are found, it defaults to a safe fallback (a local SQLite database or a temporary PostgreSQL database via testcontainers).

### Basic Usage (Auto-Detection)

To have `ConfigNoir` automatically find the best available configuration, simply call `detect_and_load_config` with no arguments.

```python
from npd_plainerflow.confignoir import ConfigNoir

# Auto-detect configuration and get an engine
settings = ConfigNoir.detect_and_load_config(verbose=True)

if settings._sql_alchemy_engine:
    engine = settings._sql_alchemy_engine
    print("Successfully connected to the database!")
    # Proceed with database operations
else:
    print(f"Failed to connect: {settings.database_connection_error_message}")

```

### Explicit Usage (Using `.env` Files)

You can also bypass auto-detection and instruct `ConfigNoir` to use one or more specific `.env` files. If the credentials in these files are invalid or incomplete, the process will fail with a `RuntimeError`.

```python
from npd_plainerflow.confignoir import ConfigNoir

# Provide a list of .env files to load
# Note: Values in later files will override earlier ones
env_files = ["path/to/your/db.env", "path/to/your/secrets.env"]

try:
    settings = ConfigNoir.detect_and_load_config(config_files=env_files, verbose=True)
    
    if settings._sql_alchemy_engine:
        engine = settings._sql_alchemy_engine
        print("Successfully connected using explicit .env files!")
    else:
        # This block will not be reached if connection fails, as it raises an exception
        pass

except RuntimeError as e:
    print(f"Failed to connect using the provided .env files: {e}")

```

## `EngineFetcher`: Engine Creation

The `EngineFetcher` class is used internally by `ConfigNoir`, but it can also be used directly if you already have a populated `Dynaconf` object.

### `EngineFetcher` Usage

```python
from npd_plainerflow.enginefetcher import EngineFetcher
from dynaconf import Dynaconf

# Assume you have a pre-configured Dynaconf object
settings = Dynaconf()
settings.DB_TYPE = "SQLITE"
settings.DB_DATABASE = ":memory:"
settings._sql_alchemy_engine = None
settings.database_connection_error_message = None


# Use EngineFetcher to create and attach the engine
EngineFetcher.get_engine(settings, verbose=True)

if settings._sql_alchemy_engine:
    print("Engine created successfully!")
else:
    print(f"Engine creation failed: {settings.database_connection_error_message}")
```

## Return Object

The `detect_and_load_config` method always returns a `Dynaconf` settings object, which will be augmented with two special attributes:

- `_sql_alchemy_engine`: Contains the ready-to-use `sqlalchemy.engine.Engine` object if the connection was successful. Otherwise, it is `None`.
- `database_connection_error_message`: A string containing an error message if the connection failed during auto-detection. It is `None` on success.
