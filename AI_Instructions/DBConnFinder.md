# PlainerFlow – `DBConnFinder`

This is a class that is part of the main plainerflow package. It should be available in the namespace after an import plainerflow statement in python.

> A lightweight “connection discovery” utility that inspects the runtime environment in a **priority order** and returns a ready-to-use SQLAlchemy `Engine` (or connection string).  
> Falls back to a local SQLite DB so the pipeline *always* has something to run against.

---

## 1. Goals
- **Zero-configuration first**: auto-detect Spark or Colab credentials whenever possible.
- **Graceful degradation**: step down to `.env` and finally SQLite without crashing.
- **Single public API**: `detect_config()` returns a live SQLAlchemy engine.

---

## 2. Public Interface

```python
class CredentialFinder:
    @staticmethod
    def detect_config(
        *, env_path: str | None = ".env", verbose: bool = False
    ) -> "sqlalchemy.engine.Engine":
        """
        Detects the best available connection source, in order of precedence,
        and returns a SQLAlchemy Engine.

        1. Spark session (Databricks / PySpark)
        2. Google Colab + Drive secrets sheet
        3. .env file (traditional secrets)
        4. Ephemeral SQLite database (fallback)

        Parameters
        ----------
        env_path : str | None
            Custom path to a .env file. If None, .env lookup is skipped.
        verbose : bool
            If True, print which source was chosen.

        Raises
        ------
        RuntimeError
            If dependencies for a detected source are missing.
        """


## 3. Discovery Order & Logic

| Priority | Source | Detection Rule | Connection Strategy |
|----------|--------|----------------|-------------------|
| 1 | Spark Session | import pyspark and SparkSession.getActiveSession() returns a session | - Read spark.conf["spark.databricks.jdbc.url"] or build a databricks+connector:// URL.<br>- Return create_engine(...). |
| 2 | Google Colab Credentials Sheet | import google.colab succeeds and a predefined Drive spreadsheet ID env var exists | - Use Google Drive API + OAuth popup to read a sheet cell containing the DB URL / password.<br>- Assemble SQLAlchemy URL and return engine. This should have a password_worksheet parameter that refers to a worksheet within the current Google Suite users Google Drive account. |
| 3 | .env Secrets File | File at env_path is present | - python-dotenv loads vars like DB_TYPE, DB_USER, DB_PASS, etc.<br>- Build create_engine(...) accordingly. |
| 4 | SQLite Fallback | All other methods fail | - Create a local DB at ~/plainerflow_fallback.db (or :memory:).<br>- Return create_engine("sqlite:///~/plainerflow_fallback.db"). |


## 4. Optional Verbose Output

When verbose=True, the method prints, e.g.:

[CredentialFinder] Using Spark session credentials.

or

[CredentialFinder] Falling back to local SQLite database: ~/plainerflow_fallback.db. This should be the default, but an optional named parameter should override this.
IF the sqlite_db_file parameter is used in this way, the sqlite db should be used no matter what else is available.


## 5. Error & Dependency Handling

- If Spark detection succeeds but pyspark or Databricks connector is missing → RuntimeError.
- If Colab detection succeeds but Google API libs (google-auth, gspread, etc.) are missing → RuntimeError with pip hint.
- If .env is chosen but required keys are absent → RuntimeError("Incomplete .env credentials").
- SQLite fallback never raises (unless disk is unwritable).

## 6. Non-Goals

- No secret rotation, key-vault integration, or AWS/GCP Secret Manager hooks (keep it minimal).
- No caching of Google Drive credentials beyond Colab's token flow.
- No multi-engine pooling or connection proxy logic.

## 7. Future Enhancements

- Support AWS Secrets Manager or Azure Key Vault as additional tiers.
- Add unit-test hooks to force a particular tier (e.g., force_source="sqlite").
- Emit structured logs instead of plain print when a logging framework is available.

---

Note: This implementation provides database connection discovery across multiple environments including Spark, Google Colab, and traditional .env file configurations.

You will need to make this package dependent on pyspark, google-colab gspread etc. Try to load these within the detection code since detecting the presence of these libraries is part of the functionality to find the relevant connection.

For the naming convention of the .env file, please use the GX_USERNAME, GX_PASSWORD, DB_DATABASE, DB_PORT, DB_HOST convention.

---

## 8. Implementation Plan

### Package Structure
- Create `plainerflow/credential_finder.py` containing the `CredentialFinder` class
- Update `plainerflow/__init__.py` to import and expose `CredentialFinder`
- Update `requirements.txt` to include optional dependencies

### Class Design
```python
class CredentialFinder:
    @staticmethod
    def detect_config(
        *, 
        env_path: str | None = ".env", 
        verbose: bool = False,
        sqlite_db_file: str | None = None,
        password_worksheet: str = "DatawarehouseUP"
    ) -> sqlalchemy.engine.Engine:
```

### Detection Logic (Priority Order)

**Priority 1: SQLite Override**
- If `sqlite_db_file` parameter is provided, use it immediately (bypassing all other detection)
- Default to `~/plainerflow_fallback.db` if no custom path specified

**Priority 2: Spark Session**
- Try `import pyspark` and check `SparkSession.getActiveSession()`
- Read `spark.conf["spark.databricks.jdbc.url"]` or build databricks connector URL
- Raise RuntimeError if pyspark missing but Spark detected

**Priority 3: Google Colab**
- Try `import google.colab` 
- Use `password_worksheet` parameter to access Google Drive spreadsheet
- Require google-auth, gspread dependencies
- Raise RuntimeError with pip hint if dependencies missing

**Priority 4: .env File**
- Check if file exists at `env_path`
- Load using python-dotenv
- Use naming convention: `GX_USERNAME`, `GX_PASSWORD`, `DB_DATABASE`, `DB_PORT`, `DB_HOST`
- Build MySQL connection string: `mysql+pymysql://...`
- Raise RuntimeError if required keys missing

**Priority 5: SQLite Fallback**
- Create `~/plainerflow_fallback.db`
- Never raises exceptions (unless disk unwritable)

### Dependencies Strategy
- Keep core dependencies minimal (only sqlalchemy in requirements.txt)
- Import optional dependencies within detection methods using try/except
- Provide clear error messages with installation hints

### Verbose Output
- Print detection source when `verbose=True`
- Format: `[CredentialFinder] Using [source] credentials.`

### Error Handling
- RuntimeError for missing dependencies when source is detected
- RuntimeError for incomplete .env credentials
- Graceful fallback to SQLite for all other failures

### Files to Create/Modify
- `plainerflow/credential_finder.py` (new)
- `plainerflow/__init__.py` (update imports)
- `requirements.txt` (add optional dependencies as comments)
