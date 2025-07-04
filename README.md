# plainerflow

A Python package for plain flow operations with SQLAlchemy integration.

## Installation

### From PyPI (recommended)

```bash
pip install plainerflow
```

### Manual Installation

If pip is not available in your environment, you can use the package by adding it to your Python path:

```python
import sys
sys.path.insert(0, "/path/to/the/right/plainerflow/subdirectory")
import plainerflow
```

## Usage

```python
import plainerflow

# Your code here
print(plainerflow.__version__)
```

## Dependencies

- SQLAlchemy >= 1.4.0

## Development

### Setting up development environment

1. Clone the repository:
```bash
git clone https://github.com/ftrotter/plainerflow.git
cd plainerflow
```

2. Create a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install development dependencies:
```bash
pip install -e ".[dev]"
```

### Building the package

```bash
python -m build
```

### Uploading to PyPI

```bash
python -m twine upload dist/*
```

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.
