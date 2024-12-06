# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations
import re
from typing import Literal

from tomlkit import comment
from tomlkit.container import Container
from tomlkit.items import (
    Bool,
    Comment,
    SingleKey,
    String,
)


class CommentNotFoundError(Exception):
    """Raised when trying to comment out a setting that does not exist"""

    def __init__(self, key: SingleKey) -> None:
        super(CommentNotFoundError, self).__init__()
        self.key = key


def comment_out(
    *,
    table_container: Container,
    key: SingleKey,
) -> None:
    """Modifies a tomlkit table container to comment out an existing setting"""

    # The _map attribute is a mapping from keys to index (int) / indices (Tuple[int]) in
    # the body attribute. This is for efficient lookup into the DOM.
    # We pop the entry because we are commenting out the setting.
    index = table_container._map.pop(key)
    dict.__delitem__(table_container, key.key)

    # index is a tuple for an AOT (array-of-tables). Otherwise an int
    assert isinstance(index, int)

    # We get a reference to the old entry. This is a tuple of the form:
    #  (key, item)
    _, prior_value = table_container.body[index]

    # Reference: https://github.com/python-poetry/tomlkit/blob/635831f1be9b0e107047e74af8ebecc7c0e4b7bf/tomlkit/container.py#L569-L577
    # We omit prior_value.trivia.trail since that is a newline
    comment_str = (
        f"{key}"
        f"{key.sep}"
        f"{prior_value.as_string()}"
        f"{prior_value.trivia.comment_ws}"
        f"{prior_value.trivia.comment}"
    )
    table_container.body[index] = (None, comment(comment_str))


def uncomment(
    *,
    table_container: Container,
    key: SingleKey,
    value: Bool | String,
    occurrence: Literal["first", "last"] = "last",
) -> None:
    """Modifies a tomlkit table container to uncomment an existing setting that was previously
    commented out."""

    commented_out_setting = re.compile(f"^#\\s*{re.escape(key.key)}\\s*=.*$")
    pos: int | None = None
    for i, kv_tuple in enumerate(table_container.body):
        table_child_key, table_child_value = kv_tuple
        if table_child_key is None and isinstance(table_child_value, Comment):
            if commented_out_setting.match(table_child_value.trivia.comment):
                pos = i
                if occurrence == "first":
                    break

    if pos is None:
        raise CommentNotFoundError(key)

    table_container.body[pos] = (key, value)
    table_container._map[key] = pos
