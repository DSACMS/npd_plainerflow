# npd_plainerflow – `ConfigNoir`

> **ConfigNoir** is a sophisticated configuration detection and management utility that has replaced the legacy `CredentialFinder`. It provides enhanced flexibility for loading configurations from multiple sources and integrates seamlessly with `EngineFetcher` for database connection management.

---

## Typical Usage Example

```python
from npd_plainerflow import ConfigNoir
import os

# Define paths to your configuration files
base_path = os.path.dirname(os.path.abspath(__file__))
env_location = os.path.abspath(os.path.join(base_path, "..", "..", ".env"))
data_settings_location = os.path.abspath(os.path.join(base_path, "..", "..", "data_file_locations.env"))

# Load settings from all specified files
settings = ConfigNoir.detect_and_load_config(
    config_files=[env_location, data_settings_location], 
    verbose=True
)

# Get the database engine
if settings._sql_alchemy_engine:
    alchemy_engine = settings._sql_alchemy_engine
    print("Connected successfully!")
else:
    raise RuntimeError(f"Failed to connect to the database: {settings.database_connection_error_message}")

# Access other configuration values
nppes_raw_schema = settings.NPPES_RAW_SCHEMA
```

## Detection Priority Order

ConfigNoir uses a sophisticated priority system that differs based on usage mode:

### Mode 1: Explicit Configuration Files

When `config_files` parameter is provided:

1. **Load specified files** in order (later files override earlier ones)
2. **Create engine** using merged configuration
3. **No fallback** - failure to load results in error

### Mode 2: Auto-Detection  

When no `config_files` specified:

1. **SQLite Override** - If `sqlite_db_file` parameter provided
2. **Spark Session** - Active Databricks/PySpark environment  
3. **Google Colab** - Google Colab environment with Drive access
4. **System Environment Variables** - Direct environment variable check
5. **Default .env** - `.env` file in current directory
6. **testcontainers PostgreSQL** - Automatic test database container
7. **SQLite Fallback** - `~/plainerflow_fallback.db`

---

## Configuration Sources & Detection Logic

### Source: Explicit Configuration Files

```python
settings = ConfigNoir.detect_and_load_config(
    config_files=[
        "/path/to/database.env",
        "/path/to/data_locations.env"
    ],
    verbose=True
)
```

**Detection**: Files must exist and be readable  
**Format**: Standard .env format  
**Merging**: Sequential loading with override (last file wins)

### Source: SQLite Override

```python
settings = ConfigNoir.detect_and_load_config(
    sqlite_db_file="~/my_project.db",
    verbose=True
)
```

**Detection**: Always used if parameter provided  
**Priority**: Overrides all other detection methods  
**Path**: Supports `~` expansion and automatic directory creation

### Source: Spark Session (Databricks)

**Detection**:

- Successful `import pyspark`
- Active SparkSession via `SparkSession.getActiveSession()`
- Available `spark.databricks.jdbc.url` configuration

**Configuration Generated**:

```python
{
    "DB_TYPE": "DATABRICKS",
    "JDBC_URL": "databricks+connector://..."
}
```

### Source: Google Colab  

**Detection**:

- Successful `import google.colab`
- Available gspread and google-auth libraries
- Accessible Google Drive spreadsheet

**Requirements**: `gspread`, `pandas`, `google-auth`  
**Authentication**: OAuth popup for Drive access  
**Configuration**: Reads from specified worksheet with structured data

**Expected Sheet Format**:

| username | password | server | port | database |
|----------|----------|--------|------|----------|
| myuser   | mypass   | db.example.com | 3306 | mydb |

### Source: System Environment Variables

**Detection**: Standard database environment variables are set in the system environment

**Required Variables**:
- **DB_TYPE** - Database type (MYSQL, POSTGRESQL, SQLITE, etc.)
- For **non-SQLite** databases: `DB_HOST`, `DB_USER`, `DB_DATABASE` are also required
- For **SQLite**: Only `DB_TYPE` and `DB_DATABASE` are required

**Optional Variables**:
- **DB_PASSWORD** - Database password
- **DB_PORT** - Database port number

**Usage Examples**:

```bash
# SQLite configuration
export DB_TYPE=SQLITE
export DB_DATABASE=~/my_project.db

# PostgreSQL configuration
export DB_TYPE=POSTGRESQL
export DB_HOST=localhost
export DB_USER=myuser
export DB_PASSWORD=mypass
export DB_PORT=5432
export DB_DATABASE=mydatabase
```

**Advantages**:
- **Security**: No sensitive credentials stored in files
- **Container-friendly**: Ideal for Docker/Kubernetes deployments
- **CI/CD Integration**: Perfect for automated deployment pipelines
- **Environment-specific**: Easy to switch between dev/staging/production

**Priority**: Environment variables take precedence over `.env` files, making them ideal for production deployments while maintaining `.env` files for local development.

### Source: Default .env File

**Detection**: `.env` file exists in current directory  
**Format**: Standard environment variable format (see Configuration File Formats section)

### Source: testcontainers PostgreSQL

**Detection**:

- Available `testcontainers` library
- Automatic container startup

**Use Case**: Development and testing environments  
**Configuration**: Automatically generated from container connection

### Source: SQLite Fallback

**Detection**: All other methods fail  
**Path**: `~/plainerflow_fallback.db`  
**Behavior**: Never fails (unless disk issues)

---

## Configuration File Formats

### Standard .env Format

```bash
# Database Connection
DB_TYPE=MYSQL
DB_USER=myuser
DB_PASSWORD=mypassword
DB_HOST=localhost
DB_PORT=3306
DB_DATABASE=mydatabase

```

### Supported Database Types

- **MYSQL**: MySQL/MariaDB connections
- **POSTGRESQL**: PostgreSQL connections  
- **SQLITE**: SQLite file databases
- **DATABRICKS**: Databricks platform (via Spark)

---

## Error Handling

### Connection Errors

When database connection fails, ConfigNoir sets:

- `settings._sql_alchemy_engine = None`
- `settings.database_connection_error_message` contains error details

### File Loading Errors

- **FileNotFoundError**: When specified config files don't exist
- **IsADirectoryError**: When config_files contains directory paths
- **TypeError**: When config_files is not a list

---

## Integration with EngineFetcher

ConfigNoir delegates actual database engine creation to `EngineFetcher`:

1. **ConfigNoir** detects and loads configuration sources
2. **EngineFetcher** creates SQLAlchemy engine based on detected configuration
3. **Engine attached** to returned Dynaconf object at `._sql_alchemy_engine`

This separation allows:

- **Modular design**: Configuration detection separate from engine creation
- **Reusable components**: EngineFetcher can be used independently  
- **Easier testing**: Mock either component independently
- **Future flexibility**: Easy to extend either configuration sources or engine types

---

## Best Practices

### 1. Use Multiple Configuration Files

```python
# Separate database credentials from application settings
settings = ConfigNoir.detect_and_load_config(
    config_files=[
        "database_credentials.env",  # Database connection info
        "app_settings.env",         # Application-specific settings
        "local_overrides.env"       # Local development overrides
    ],
    verbose=True
)
```

### 2. Always Check Engine Connection

```python
settings = ConfigNoir.detect_and_load_config(verbose=True)
if not settings._sql_alchemy_engine:
    raise RuntimeError(f"Database connection failed: {settings.database_connection_error_message}")
```

### 3. Use Verbose Mode During Development

```python
# Enable verbose logging to understand which configuration source is used
settings = ConfigNoir.detect_and_load_config(verbose=True)
```

### 4. Environment-Specific Configuration

```python
# Different configurations for different environments
import os
environment = os.getenv('ENVIRONMENT', 'development')

if environment == 'production':
    config_files = ['production.env']
elif environment == 'testing':
    # Use testcontainers PostgreSQL or SQLite
    settings = ConfigNoir.detect_and_load_config(verbose=True)
else:
    config_files = ['development.env', 'local_overrides.env']
    
if 'config_files' in locals():
    settings = ConfigNoir.detect_and_load_config(
        config_files=config_files, 
        verbose=True
    )
```

---

## Advanced Features

### Dynamic Configuration Loading

```python
# Load different configurations based on runtime conditions
import os

def get_config_for_environment():
    env = os.getenv('APP_ENV', 'development')
    
    config_files = ['base.env']  # Always load base config
    
    if env == 'production':
        config_files.append('production.env')
    elif env == 'staging':
        config_files.append('staging.env')
    else:
        config_files.extend(['development.env', 'local.env'])
    
    return ConfigNoir.detect_and_load_config(
        config_files=config_files,
        verbose=True
    )

settings = get_config_for_environment()
```

### Custom SQLite Paths

```python
# Use project-specific SQLite database
import os
from pathlib import Path

project_root = Path(__file__).parent.parent
sqlite_path = project_root / "data" / "project.db"

settings = ConfigNoir.detect_and_load_config(
    sqlite_db_file=str(sqlite_path),
    verbose=True
)
```

---

## Troubleshooting

### Common Issues

**Issue**: `TypeError: Expected 'config_files' to be a list of paths`  
**Solution**: Ensure config_files parameter is a list, not a single string

```python
# Wrong
settings = ConfigNoir.detect_and_load_config(config_files=".env")

# Correct  
settings = ConfigNoir.detect_and_load_config(config_files=[".env"])
```

**Issue**: `FileNotFoundError: Missing configuration file(s)`  
**Solution**: Verify file paths exist and are accessible

```python
import os
config_file = "my_config.env"
if not os.path.exists(config_file):
    print(f"Config file not found: {config_file}")
```

**Issue**: `settings._sql_alchemy_engine is None`  
**Solution**: Check the error message and verify configuration values

```python
if not settings._sql_alchemy_engine:
    print(f"Connection failed: {settings.database_connection_error_message}")
    # Review your database configuration values
```

**Issue**: Google Colab authentication fails  
**Solution**: Ensure proper Google authentication and Drive permissions

```python
# In Google Colab, run this first:
from google.colab import auth
auth.authenticate_user()
```

### Debug Mode

Always use `verbose=True` during development to understand which configuration source is being used:

```python
settings = ConfigNoir.detect_and_load_config(verbose=True)
# Output examples:
# [ConfigNoir] Using .env file credentials from /path/to/.env.
# [ConfigNoir] Using Spark session credentials.
# [ConfigNoir] Falling back to local SQLite database: /Users/username/plainerflow_fallback.db
```
