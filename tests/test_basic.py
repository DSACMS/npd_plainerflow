"""
Basic tests for plainerflow package
"""
import pytest


def test_import_plainerflow():
    """Test that plainerflow can be imported"""
    import plainerflow
    assert plainerflow is not None


def test_version_exists():
    """Test that version is available"""
    import plainerflow
    assert hasattr(plainerflow, '__version__')
    assert plainerflow.__version__ is not None


def test_sqlalchemy_dependency():
    """Test that sqlalchemy is available (our main dependency)"""
    try:
        import sqlalchemy
        assert sqlalchemy is not None
    except ImportError:
        pytest.fail("SQLAlchemy should be available as a dependency")
