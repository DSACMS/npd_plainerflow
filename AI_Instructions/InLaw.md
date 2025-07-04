# PlainerFlow – **`InLaw` Validation Framework**

A lightweight wrapper around **Great Expectations (GX)** that lets you define simple pass/fail
tests as Python subclasses—no DataDocs, suites, or YAML required.

This is a class that is part of the main plainerflow package. It should be available in the namespace after an import plainerflow statement in python.

---

## 1. Concept & Goals
- **"In-Law" pattern**: Tests run *after* your main pipeline—loudly complain but never block.  
- **Single-file child classes**: Each test lives in its own file for easy AI-generation and review.  
- **Zero GX boilerplate**: Parent class handles Spark/SQLAlchemy → GX DataFrame conversion.  
- **ANSI color console output**: Green = pass, Red = fail (plus error message).  
- **Aggregated summary**: Print total passes / failures at the end.

---

## 2. Parent Class – `InLaw`

| Category | Requirement |
|----------|-------------|
| **Core API** | `@staticmethod InLaw.run_all(engine)` → auto-discovers subclasses, instantiates, runs each test, prints results, and final tally. |
| **Test Discovery** | Iterate over `InLaw.__subclasses__()` to find child classes in current namespace. |
| **GX Helpers** | Provide static helpers:<br>• `to_gx_dataframe(sql: str, engine) -> gx.DataFrame`<br>• `ansi_green(text)` / `ansi_red(text)` for color printing |
| **Abstract Contract** | Require each child to implement: <br>`@staticmethod def run(engine) -> bool | str`<br>&nbsp;&nbsp;• **Return `True`**  → test passed<br>&nbsp;&nbsp;• **Return `str`** → test failed, string = error message |
| **Metadata** | Child classes must define a class attribute `title: str` used for friendly printout. |
| **Fallbacks** | If GX import fails, raise `ImportError` with install hint. |
| **No Side Effects** | Parent never commits/rolls back: calling code owns transaction scope. |

---

## 3. Child Class Template

```python
from plainerflow import InLaw

class InLawExpectFewerThanThousandRows(InLaw):
    title = "Ensure table has < 1,000 rows"

    @staticmethod
    def run(engine):
        # 1) Compose SQL
        sql = "SELECT COUNT(*) AS n FROM books"
        # 2) Convert to GX dataframe
        gdf = InLaw.to_gx_dataframe(sql, engine)
        # 3) Run expectation (open-source GX expectation)
        result = gdf.expect_column_values_to_be_between(
            column="n", min_value=0, max_value=999
        )
        # 4) Pass/fail
        if result.success:
            return True
        return f"Row count = {gdf.iloc[0]['n']}"

### Key points

- title and run() are the only things child classes must provide.
- AI agents can implement new tests by editing copies of this template—no global context needed.

---

## 4. Execution Flow (InLaw.run_all(engine))

1. Discover tests → iterate over subclasses.
2. For each test
   1. Print Running: {title}
   2. Call child.run(engine) inside try/except:
      - True → print green "PASS"
      - str  → print red "FAIL" + message
      - Exception → print red "ERROR" + exception text
3. Summary → X passed · Y failed · Z errors

---

## 5. Console Output Example

===== IN-LAW TESTS =====
Running: Ensure table has < 1,000 rows
PASS
Running: Check authors table has no null names
FAIL: 12 null values found in column "name"
============================================
Summary: 1 passed · 1 failed

(Green PASS and red FAIL/ ERROR lines use ANSI color codes.)

---

## 6. Extensibility & Roadmap

- Tags / groups: Allow child classes to set tags = {"perf", "schema"} for selective runs.
- --dry-run flag: Execute SQL but skip GX expectations, showing row samples.
- Parallel execution: Thread or asyncio pool for large suites.
- Plugin expectations: Auto-register custom GX expectations located alongside child classes.

---
