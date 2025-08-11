"""
npd_plainerflow - A Python package for plain flow operations
"""

import warnings

# Suppress Marshmallow warnings before importing InLaw (which imports Great Expectations)
# These are version compatibility issues between Great Expectations and Marshmallow
# that will be resolved in future GX releases
try:
    from marshmallow.warnings import ChangedInMarshmallow4Warning
    warnings.filterwarnings(
        "ignore",
        message=r".*Number.*field should not be instantiated.*Use.*Integer.*Float.*or.*Decimal.*instead.*",
        category=ChangedInMarshmallow4Warning
    )
except ImportError:
    # Fallback if marshmallow warnings module structure changes
    warnings.filterwarnings(
        "ignore",
        message=r".*Number.*field should not be instantiated.*Use.*Integer.*Float.*or.*Decimal.*instead.*"
    )

__version__ = "0.1.0"
__author__ = "Fred Trotter"
__email__ = "fred.trotter@gmail.com"

# Import all main classes/functions here so they're available when someone does:
# import npd_plainerflow

from .credential_finder import CredentialFinder
from .inlaw import InLaw
from .dbtable import DBTable, DBTableError, DBTableValidationError, DBTableHierarchyError
from .sqloopcicle import SQLoopcicle
from .frostdict import FrostDict, FrozenKeyError
from .confignoir import ConfigNoir
from .enginefetcher import EngineFetcher


# As we add more classes later, they should be imported here
# Example:
# from .some_module import SomeClass
# from .another_module import AnotherClass

__all__ = [
    "__version__", 
    "CredentialFinder", 
    "InLaw", 
    "DBTable", 
    "DBTableError", 
    "DBTableValidationError", 
    "DBTableHierarchyError",
    "SQLoopcicle",
    "FrostDict",
    "FrozenKeyError",
    "ConfigNoir",
    "EngineFetcher"
]
