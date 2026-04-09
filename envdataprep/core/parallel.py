"""
Parallel processing utilities for envdataprep.

``map_items`` runs a single-item function over many items. Use ``workers > 1``
for process-based parallelism; otherwise execution is sequential in-process.

Most users should rely on ``@enable_parallel`` on format helpers rather than
calling ``map_items`` / ``batch_process`` directly.
"""

from collections.abc import Callable
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import Any

from tqdm import tqdm


def _run_one_item(
    payload: tuple[int, Any, Callable[..., Any], dict[str, Any]],
) -> tuple[int, Any, Any | None, str | None]:
    """Execute ``process_func(item, **func_kwargs)``; used in worker processes."""
    idx, item, process_func, func_kwargs = payload
    try:
        result = process_func(item, **func_kwargs)
        return idx, item, result, None
    except Exception as e:
        return idx, item, None, str(e)


def map_items(
    items: list[Any],
    process_func: Callable[..., Any],
    func_kwargs: dict[str, Any] | None = None,
    *,
    workers: int | None = None,
    show_progress: bool = True,
) -> list[tuple[Any, Any | None, str | None]]:
    """Run ``process_func`` on each item; preserve input order in the result.

    Parameters
    ----------
    items : list[Any]
        Items to process (e.g. file paths), in order.
    process_func : Callable
        Callable taking ``item`` as the first positional argument.
    func_kwargs : dict[str, Any], optional
        Extra keyword arguments passed to ``process_func`` for every item.
    workers : int, optional
        If ``None`` or ``1``, run sequentially in the current process.
        If greater than ``1``, use a :class:`ProcessPoolExecutor` with that
        many workers. On HPC, set this to match allocated CPUs
        (e.g. ``SLURM_CPUS_PER_TASK``).
    show_progress : bool, default True
        Show a tqdm bar when ``workers > 1``.

    Returns
    -------
    list[tuple[Any, Any | None, str | None]]
        One row per input item, in the same order as ``items``.
        Each row is ``(item, result, error)``.
        If ``error`` is ``None``, the call succeeded and ``result`` is the
        return value (which may be ``False`` for boolean checks).
        If ``error`` is set, ``result`` is ``None`` and ``error`` is a message.

    Examples
    --------
    >>> from envdataprep.core.parallel import map_items
    >>> from envdataprep.core.netcdf import subset_netcdf
    >>>
    >>> files = ["a.nc", "b.nc"]
    >>> rows = map_items(
    ...     files,
    ...     subset_netcdf,
    ...     {"output_dir": "out/", "include_vars": ["t"]},
    ...     workers=2,
    ... )
    """
    # Ensure func_kwargs is a dictionary
    if func_kwargs is None:
        func_kwargs = {}

    # Get the number of items
    n = len(items)

    # Return empty list if there are no items
    if n == 0:
        return []

    # Run sequentially if workers is None or <= 1
    if workers is None or workers <= 1:
        rows: list[tuple[Any, Any | None, str | None]] = []
        for item in items:
            try:
                r = process_func(item, **func_kwargs)
                rows.append((item, r, None))
            except Exception as e:
                rows.append((item, None, str(e)))
        return rows

    # Run in parallel if workers > 1
    workers = min(workers, n)
    payloads = [
        (i, item, process_func, func_kwargs) for i, item in enumerate(items)
    ]

    with ProcessPoolExecutor(max_workers=workers) as executor:
        future_map = {
            executor.submit(_run_one_item, p): p[0] for p in payloads
        }
        iterator = as_completed(future_map)
        if show_progress:
            iterator = tqdm(
                iterator, total=n, desc="Processing"
            )

        by_idx: dict[int, tuple[Any, Any | None, str | None]] = {}
        for fut in iterator:
            idx, item, result, err = fut.result()
            by_idx[idx] = (item, result, err)

    return [by_idx[i] for i in range(n)]
