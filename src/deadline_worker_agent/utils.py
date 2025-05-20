# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations

from collections.abc import MutableMapping
import os
from pathlib import Path
from types import TracebackType
from typing import Any, Callable, Generic, Iterator, TypeVar

_K = TypeVar("_K")
_V = TypeVar("_V")


class MappingWithCallbacks(MutableMapping, Generic[_K, _V]):
    """
    A dict-like class that allows callbacks which are invoked after common dict operations
    (get, set, del)
    """

    _dict: dict[_K, _V]
    _setitem_callback: Callable[[_K, _V], None] | None
    _getitem_callback: Callable[[_K], None] | None
    _delitem_callback: Callable[[_K], None] | None

    def __init__(
        self,
        *args,
        setitem_callback: Callable[[_K, _V], None] | None = None,
        getitem_callback: Callable[[_K], None] | None = None,
        delitem_callback: Callable[[_K], None] | None = None,
        **kwargs,
    ) -> None:
        self._dict = dict()
        self._setitem_callback = setitem_callback
        self._getitem_callback = getitem_callback
        self._delitem_callback = delitem_callback
        self.update(dict(*args, **kwargs))

    def __delitem__(self, __key: _K) -> None:
        if self._delitem_callback:
            self._delitem_callback(__key)
        del self._dict[__key]

    def __getitem__(self, __key: _K) -> _V:
        if self._getitem_callback:
            self._getitem_callback(__key)
        return self._dict[__key]

    def __setitem__(self, __key: _K, __value: _V) -> None:
        if self._setitem_callback:
            self._setitem_callback(__key, __value)
        self._dict[__key] = __value

    def __iter__(self) -> Iterator:
        return iter(self._dict)

    def __len__(self) -> int:
        return len(self._dict)


class FileContext:
    """File context, ensures a file is deleted."""

    def __init__(self, file_path: str, delete_existing: bool = False):
        self._file_path = file_path
        self._delete_existing = delete_existing

    def __enter__(self):
        if self._delete_existing:
            Path(self._file_path).unlink(missing_ok=True)
        return self._file_path

    def __exit__(
        self,
        type: TypeVar,
        value: Any,
        traceback: TracebackType,
    ):
        # Delete the file
        if self._file_path and os.path.exists(self._file_path):
            os.remove(self._file_path)
        return False
