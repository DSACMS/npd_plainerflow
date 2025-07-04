# plainerflow

This is a python package that should be downloaded from PyPI using pip with:

```bash
pip install plainerflow
```

Which will result in us being able to simply say:

```python
import plainerflow
```

To get all of the sub-classes we will define later into the namespace.

However the typical installation environment will not have pip available. So it needs to be hard coded to work as:

```python
import sys
sys.path.insert(0, "/path/to/the/right/plainerflow/subdirectory")
import plainerflow
```

We will be adding multiple classes to this project later on and many of them will be dependent on sqlalchemy, so let's setup a project wide dependence on sqlalchemy.

Please setup all of the various scripts and tools needed to get this working including the requirements file for pip and the standard upload infrastructure for a python project hosted on PyPI.
