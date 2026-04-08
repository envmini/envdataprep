"""
Parallel processing utilities for envdataprep.

Provides batch_process for parallelizing any single-item function
using multiprocessing. Most users should use the @enable_parallel
decorator instead of calling batch_process directly.
"""

import os
from typing import Callable, Any
from concurrent.futures import ProcessPoolExecutor, as_completed
from functools import partial

from tqdm import tqdm


def _process_single_item(item: Any, process_func: Callable[..., Any], func_kwargs: dict[str, Any] | None = None):
    """Wrapper to handle exceptions for single item processing."""
    try:
        result = process_func(item, **func_kwargs)
        return item, True, result, None
    except Exception as e:
        return item, False, None, str(e)


def batch_process(
    items: list[Any],
    process_func: Callable[..., Any],
    func_kwargs: dict[str, Any] | None = None,
    max_workers: int | None = None,
    show_progress: bool = True,
) -> tuple[list[Any], list[tuple[Any, str]]]:
    """Process a list of items in parallel using multiprocessing.

    Parameters
    ----------
    items : list[Any]
        List of items to process (e.g., file paths).
    process_func : Callable
        Function to apply to each item. Must accept item as first argument.
    func_kwargs : dict[str, Any], optional
        Additional keyword arguments to pass to process_func.
    max_workers : int, optional
        Number of parallel workers. If None, uses os.cpu_count().
        On HPC clusters, set this to match your allocated CPUs
        (e.g., via SLURM_CPUS_PER_TASK, PBS_NP).
    show_progress : bool, default True
        Whether to show a tqdm progress bar.

    Returns
    -------
    tuple[list[Any], list[tuple[Any, str]]]
        (successful_items, failed_items_with_errors)

    Examples
    --------
    >>> from envdataprep.core.parallel import batch_process
    >>> from envdataprep.core.netcdf import subset_netcdf
    >>>
    >>> files = ["file1.nc", "file2.nc", "file3.nc"]
    >>> kwargs = {
    ...     "output_dir": "output/",
    ...     "include_vars": ["PRODUCT/latitude", "PRODUCT/longitude"],
    ... }
    >>>
    >>> successful, failed = batch_process(
    ...     files, subset_netcdf, func_kwargs=kwargs, max_workers=4
    ... )
    """
    func_kwargs = func_kwargs or {}
    max_workers = max_workers or os.cpu_count() or 1

    successful = []
    failed = []

    worker_func = partial(
        _process_single_item,
        process_func=process_func,
        func_kwargs=func_kwargs,
    )

    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(worker_func, item): item for item in items}

        iterator = as_completed(futures)
        if show_progress:
            iterator = tqdm(
                iterator, total=len(items), desc="Processing"
            )

        for future in iterator:
            item, success, result, error = future.result()
            if success:
                successful.append(item)
            else:
                failed.append((item, error))

    return successful, failed
