"""Utility decorators for envdataprep."""

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


def enable_parallel(func: Callable[..., Any]) -> Callable[..., Any]:
    """Dispatch multi-item calls through :func:`map_items`.

    The wrapped function must:

    1. Take a single *item* as its first parameter (e.g. one file path).
    2. Declare ``workers: int | None = None`` and
       ``show_progress: bool = True`` so batch calls can control execution.

    If the first argument is a ``list`` or ``tuple``, each element is processed
    with the remaining bound arguments. The return value is a **list of rows**,
    same order as the input sequence::

        [(item, result, error), ...]

    where ``error`` is ``None`` on success. For a single-item first argument
    (e.g. ``str`` path), the body runs once and returns that function's normal
    return value; ``workers`` is ignored.

    Parallelism is used only when ``workers`` is greater than ``1``.
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        sig = inspect.signature(func)
        bound = sig.bind(*args, **kwargs)
        bound.apply_defaults()
        arguments = dict(bound.arguments)

        first_param = next(iter(sig.parameters))
        first_arg = arguments[first_param]

        if not isinstance(first_arg, (list, tuple)):
            return func(*args, **kwargs)

        items = list(first_arg)
        if not items:
            return []

        workers = arguments.pop("workers", None)
        show_progress = arguments.pop("show_progress", True)

        func_kwargs: dict[str, Any] = {}
        for name, value in arguments.items():
            if name == first_param:
                continue
            param = sig.parameters[name]
            if param.kind == inspect.Parameter.VAR_KEYWORD:
                func_kwargs.update(value)
            elif param.kind == inspect.Parameter.VAR_POSITIONAL:
                continue
            else:
                func_kwargs[name] = value

        from ..core.parallel import map_items

        return map_items(
            items=items,
            process_func=func,
            func_kwargs=func_kwargs,
            workers=workers,
            show_progress=show_progress,
        )

    return wrapper
