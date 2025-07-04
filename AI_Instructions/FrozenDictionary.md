# FrostDict
FrostDict is a frozen dictionary (at the top dictionary level)

This is a class that is part of the main plainerflow package. It should be available in the namespace after an `import plainerflow` statement in python and should not need to be specifically imported.

The files should be called plainerflow/frostdict.py

This should specifically work with and be tested with the SQL loop classes as described in AI_Instructions/SQLoop.md and implemented in plainerflow/sqloopcicle.py

## 1. Scope & Goals
- Provide the **thinnest possible abstraction** over SQLAlchemy for batch execution of templated SQL.
- All configuration and query text lives in a **Frozen Dictionary** object.
- Emphasis on **immutability, clarity, and crash-fast behavior** rather than full-featured orchestration.

---

## 2. Core Component – `FrostDict`
### Purpose
A lightweight container for configuration and SQL templates that:
1. **Feels like a normal `dict` for reads.**  
2. **Prevents accidental re-assignment of top-level keys.**

### Required Behaviors
| Aspect | Requirement |
|--------|-------------|
| API surface | `[]` get access, `.keys()`, `.items()`, iteration over keys |
| Top-level write lock | Any attempt to set or update an existing **top-level** key **must raise an error** (e.g., `FrozenKeyError`) |
| First insertion | Adding a brand-new top-level key is allowed |
| Nested mutability | Values *inside* a top-level key (e.g., nested dicts, lists) are **not locked** by default |
| Accepted value types | `str` (f-string SQL), nested `dict`, `list`, or arbitrary Python objects |
| Hashability | The object itself is **hashable** if (and only if) all contained values are hashable |
| Repr / str | Clear, one-line “FrostDict({...})” for debugging |

### Error Handling
- **`FrozenKeyError`** (custom): raised on duplicate top-level assignment.
- Underlying mutable operations (e.g., modifying a list stored as a value) are *not* trapped—caller responsibility.

---

## 3. Usage Workflow

**Define** a `FrostDict` at module load time:

```python
QUERIES = FrostDict({
    "create_schema": f"CREATE SCHEMA IF NOT EXISTS {schema_name}",
    "book_by_id":    "SELECT * FROM books WHERE id = :book_id",
    "all_authors":   "SELECT * FROM authors",
})

# or

sql = FrostDict()
sql['this is my key'] = f"This is my fancy SQL"
```

Tests should test that attempts to rewrite frozen keys fails and that different syntax for adding data function as expected.
