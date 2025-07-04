Here is your request formatted as a proper Markdown document:

# SQLoopcicle


A single-purpose utility for looping over a mapping of SQL statements and running them using a SQLAlchemy engine. Built for clarity, testability, and optional dry-run execution.

This is a class that is part of themain plainerflow package. It should be available in the namespace after an import plainerflow statement in python.

The frozen dictionary that this will commonly be used with is described in AI_Instructions/FrozenDictionary.md

The class file name should be sqloopcicle.py

---

## ✅ Purpose

- Provide a simple loop-and-run mechanism for executing SQL statements from a dictionary.
- Support both **actual execution** and **dry-run mode** with clear console output.
- Minimal, functional abstraction aligned with PlainerFlow’s "no-fluff" philosophy.

---

## 🧱 Class: `SQLoopcicle`

### Static Method

```python
SQLoopcicle.run_sql_loop(
    sql_dict: dict[str, str],
    engine,
    *,
    is_just_print: bool = False
) -> None


⸻

📥 Parameters

Name	Type	Description
sql_dict	dict[str, str]	Mapping of keys to SQL statements. These statements are executed in dictionary order.
engine	sqlalchemy.engine.Engine	A live SQLAlchemy engine for database interaction.
is_just_print	bool (keyword-only)	If True, SQL statements are printed instead of executed.


⸻

🔁 Execution Logic
	1.	Startup Message
	•	If is_just_print == True:

===== DRY-RUN MODE – NO SQL WILL BE EXECUTED =====


	•	If False:

===== EXECUTING SQL LOOP =====


	2.	Loop Through SQL Dictionary
For each (key, sql_string) in sql_dict.items():
	•	Print:

▶ key_name: SQL_STATEMENT_HERE


	•	If is_just_print == False, execute the SQL:

with engine.begin() as conn:
    conn.execute(text(sql_string)) # use conn.execute(text(sql_string)) for safety


	3.	End Message
	•	If is_just_print == True:

===== I AM NOT RUNNING SQL =====


	•	If False:

===== SQL LOOP COMPLETE =====



⸻

🚨 Error Handling
	•	Execution errors are not swallowed — they propagate normally.
	•	Caller is responsible for handling exceptions or wrapping the call in try/except.

⸻

🚫 Non-Goals
	•	No templating or SQL rendering — f-strings or formatting must be handled before passing to run_sql_loop.
	•	No parameter binding or transaction nesting.
	•	No Airflow-style DAG logic, no retry mechanisms, no logging plugins.

⸻

🌱 Future Enhancements (Optional)
	•	verbose mode for printing execution time and success indicators.
	•	Add a second argument (params_dict) to allow parameterized SQL execution.
	•	on_error hook or strategy pattern for retry, skip, or fail-fast behavior.

---

## 📋 Implementation Plan

### Files to Create/Modify:
1. **`plainerflow/sqloopcicle.py`** - Main implementation
2. **`plainerflow/__init__.py`** - Add SQLoopcicle import/export
3. **`tests/test_sqloopcicle.py`** - Comprehensive test suite

### Implementation Details:
- **Class name**: `SQLoopcicle` 
- **Method signature**: `SQLoopcicle.run_sql_loop(sql_dict: dict[str, str], engine, *, is_just_print: bool = False) -> None`
- **SQL execution**: Uses `conn.execute(text(sql_string))` for safety
- **Dictionary support**: Accepts any Python `dict[str, str]`
- **Output formatting**: Exact console messages as specified above
- **Error handling**: Lets exceptions propagate naturally (no swallowing)
- **Code style**: Follows existing plainerflow patterns with docstrings and type hints

### Test Coverage:
- Dry-run mode functionality
- Actual SQL execution mode
- Output message formatting
- Error propagation
- Integration with SQLAlchemy engines
- Edge cases (empty dict, malformed SQL)
