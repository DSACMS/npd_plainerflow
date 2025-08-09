"""
Basic tests for npd_plainerflow package
"""
import pytest


def test_import_npd_plainerflow():
    """Test that npd_plainerflow can be imported"""
    import npd_plainerflow
    assert npd_plainerflow is not None


def test_version_exists():
    """Test that version is available"""
    import npd_plainerflow
    assert hasattr(npd_plainerflow, '__version__')
    assert npd_plainerflow.__version__ is not None


def test_sqlalchemy_dependency():
    """Test that sqlalchemy is available (our main dependency)"""
    try:
        import sqlalchemy
        assert sqlalchemy is not None
    except ImportError:
        pytest.fail("SQLAlchemy should be available as a dependency")
