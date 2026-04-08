"""Utility decorators for envdataprep."""

import inspect
from functools import wraps


def handle_file_errors(func):
    """Decorator to handle common file I/O errors with clear messages."""
    @wraps(func)
    def wrapper(path: str, *args, **kwargs):
        try:
            return func(path, *args, **kwargs)
        except FileNotFoundError as exc:
            raise FileNotFoundError(
                f"File not found: {path}"
            ) from exc
        except PermissionError as exc:
            raise PermissionError(
                f"Permission denied accessing file: {path}"
            ) from exc
        except (OSError, IOError) as exc:
            raise OSError(
                f"Cannot open '{path}'"
            ) from exc
    return wrapper


def enable_parallel(func):
    """Add multi-item parallel support to a single-item function.

    The decorated function must:
    1. Accept a single item as its first parameter.
    2. Include ``workers: int | None = None`` and
       ``show_progress: bool = True`` in its signature.

    When called with a list as the first argument, the decorator
    dispatches to batch_process (parallel) or a sequential loop.
    For single-item calls, the function runs normally.
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        sig = inspect.signature(func)
        bound = sig.bind(*args, **kwargs)
        bound.apply_defaults()
        arguments = bound.arguments

        first_param = next(iter(sig.parameters))
        first_arg = arguments[first_param]

        if not isinstance(first_arg, list):
            return func(*args, **kwargs)

        workers = arguments.pop("workers", None)
        show_progress = arguments.pop("show_progress", True)

        func_kwargs = {}
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

        if workers is not None and workers > 1:
            from ..core.parallel import batch_process
            return batch_process(
                items=first_arg,
                process_func=func,
                func_kwargs=func_kwargs,
                max_workers=workers,
                show_progress=show_progress,
            )

        successful = []
        failed = []
        for item in first_arg:
            try:
                result = func(item, **func_kwargs)
                successful.append(result)
            except Exception as e:
                failed.append((item, str(e)))
        return successful, failed

    return wrapper
