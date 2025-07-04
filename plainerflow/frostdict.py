"""
FrostDict - A frozen dictionary (at the top dictionary level).

A lightweight container for configuration and SQL templates that feels like a normal
dict for reads but prevents accidental re-assignment of top-level keys.
Emphasis on immutability, clarity, and crash-fast behavior.
"""

from typing import Any, Dict, Iterator, KeysView, ItemsView, Union
from collections.abc import Mapping


class FrozenKeyError(Exception):
    """
    Custom exception raised when attempting to reassign an existing top-level key
    in a FrostDict.
    """
    pass


class FrostDict(Mapping[str, Any]):
    """
    A frozen dictionary that prevents re-assignment of existing top-level keys.
    
    Feels like a normal dict for reads, but raises FrozenKeyError on attempts
    to update existing top-level keys. New keys can be added, and nested values
    remain mutable.
    
    Examples:
        >>> fd = FrostDict({'key1': 'value1'})
        >>> fd['key1']  # Reading works normally
        'value1'
        >>> fd['key2'] = 'value2'  # Adding new keys works
        >>> fd['key1'] = 'new_value'  # This raises FrozenKeyError
        FrozenKeyError: Cannot reassign existing key 'key1' in FrostDict
        
        >>> # Nested values remain mutable
        >>> fd = FrostDict({'config': {'setting': 'value'}})
        >>> fd['config']['setting'] = 'new_value'  # This works fine
    """
    
    def __init__(self, initial_data: Union[Dict[str, Any], None] = None):
        """
        Initialize a FrostDict with optional initial data.
        
        Args:
            initial_data: Optional dictionary to initialize with.
        """
        self._data: Dict[str, Any] = {}
        if initial_data is not None:
            self._data.update(initial_data)
    
    def __getitem__(self, key: str) -> Any:
        """Get an item by key."""
        return self._data[key]
    
    def __setitem__(self, key: str, value: Any) -> None:
        """
        Set an item by key.
        
        Raises FrozenKeyError if the key already exists.
        """
        if key in self._data:
            raise FrozenKeyError(f"Cannot reassign existing key '{key}' in FrostDict")
        self._data[key] = value
    
    def __contains__(self, key: str) -> bool:
        """Check if key exists in the dictionary."""
        return key in self._data
    
    def __iter__(self) -> Iterator[str]:
        """Iterate over keys."""
        return iter(self._data)
    
    def __len__(self) -> int:
        """Return the number of items."""
        return len(self._data)
    
    def __repr__(self) -> str:
        """Return a clear, one-line representation for debugging."""
        return f"FrostDict({self._data!r})"
    
    def __str__(self) -> str:
        """Return string representation."""
        return self.__repr__()
    
    def __eq__(self, other: object) -> bool:
        """Check equality with another FrostDict or dict."""
        if isinstance(other, FrostDict):
            return self._data == other._data
        elif isinstance(other, dict):
            return self._data == other
        return False
    
    def __hash__(self) -> int:
        """
        Return hash if all contained values are hashable.
        
        Raises TypeError if any value is not hashable.
        """
        try:
            # Create a tuple of sorted key-value pairs for consistent hashing
            items = tuple(sorted(self._data.items()))
            return hash(items)
        except TypeError as e:
            raise TypeError(f"FrostDict is not hashable because it contains unhashable values: {e}")
    
    def keys(self) -> KeysView[str]:
        """Return a view of the dictionary's keys."""
        return self._data.keys()
    
    def items(self) -> ItemsView[str, Any]:
        """Return a view of the dictionary's items."""
        return self._data.items()
    
    def values(self):
        """Return a view of the dictionary's values."""
        return self._data.values()
    
    def get(self, key: str, default: Any = None) -> Any:
        """Get a value by key with optional default."""
        return self._data.get(key, default)
