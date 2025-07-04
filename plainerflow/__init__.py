"""
plainerflow - A Python package for plain flow operations
"""

__version__ = "0.1.0"
__author__ = "Fred Trotter"
__email__ = "fred.trotter@gmail.com"

# Import all main classes/functions here so they're available when someone does:
# import plainerflow

from .credential_finder import CredentialFinder
from .inlaw import InLaw
from .dbtable import DBTable, DBTableError, DBTableValidationError, DBTableHierarchyError
from .sqloopcicle import SQLoopcicle
from .frostdict import FrostDict, FrozenKeyError

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
    "FrozenKeyError"
]
