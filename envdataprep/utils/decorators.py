"""Utility decorators."""

import inspect
from collections.abc import Callable
from functools import wraps
from typing import Any


def handle_file_errors(func: Callable[..., Any]) -> Callable[..., Any]:
    """Decorator to handle common file I/O errors with clear messages.

    The wrapped function's first parameter is treated as the file path (any name,
    e.g. ``input_path`` or ``path``); calls may use positional or keyword args.
    """
    sig = inspect.signature(func)
    first_param = next(iter(sig.parameters))

    def _first_path_value(a: tuple, kw: dict[str, Any]) -> Any:
        try:
            return sig.bind(*a, **kw).arguments[first_param]
        except TypeError:
            return kw.get(first_param, a[0] if a else "?")

    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except FileNotFoundError as exc:
            path = _first_path_value(args, kwargs)
            raise FileNotFoundError(
                f"File not found: {path}"
            ) from exc
        except PermissionError as exc:
            path = _first_path_value(args, kwargs)
            raise PermissionError(
                f"Permission denied accessing file: {path}"
            ) from exc
        except (OSError, IOError) as exc:
            path = _first_path_value(args, kwargs)
            raise OSError(
                f"Cannot open '{path}'"
            ) from exc
    return wrapper
