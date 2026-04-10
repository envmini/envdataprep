"""Parallel processing utilities."""

import os

from collections.abc import Callable
from concurrent.futures import ProcessPoolExecutor, as_completed
from functools import partial
from typing import Any

from tqdm import tqdm


def _process_single_file(file_path, process_func, func_kwargs):
    """Wrapper to handle exceptions for single file processing."""
    try:
        result = process_func(file_path, **func_kwargs)
        return file_path, True, result, None
    except Exception as e:
        return file_path, False, None, str(e)


def process_files_parallel(
    files: list[str],
    process_func: Callable,
    func_kwargs: dict[str, Any] | None = None,
    max_workers: int | None = None,
    show_progress: bool = True,
) -> tuple[list[str], list[tuple[str, str]]]:
    """Process multiple files in parallel.

    Parameters
    ----------
    files : list[str]
        List of file paths to process.
    process_func : Callable
        Function to apply to each file. Must accept file_path as first argument.
    func_kwargs : Dict[str, Any], optional
        Additional keyword arguments to pass to process_func.
    max_workers : int, optional
        Number of parallel workers. If None, uses os.cpu_count().
    show_progress : bool, default True
        Whether to show progress bar (requires tqdm).

    Returns
    -------
    tuple[list[str], list[tuple[str, str]]]
        (successful_files, failed_files_with_errors)
    """
    func_kwargs = func_kwargs or {}

    # If max_workers is not set, use the number of CPUs - 1, but not more than the number of files
    if max_workers is None:
        max_workers = os.cpu_count() - 1

    # The max_workers should not be greater than the number of files
    max_workers = min(max_workers, len(files))

    # Initialize lists to store successful and failed files
    successful = []
    failed = []

    # Use partial to bind arguments
    worker_func = partial(_process_single_file, process_func=process_func, func_kwargs=func_kwargs)

    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(worker_func, f): f for f in files}

        if show_progress:
            try:
                iterator = tqdm(as_completed(futures), total=len(files), desc="Processing")
            except ImportError:
                print("Install tqdm for progress bar: pip install tqdm")
                iterator = as_completed(futures)
        else:
            iterator = as_completed(futures)

        for future in iterator:
            file_path, success, result, error = future.result()
            if success:
                successful.append(file_path)
            else:
                failed.append((file_path, error))
                if show_progress:
                    print(f"\nFailed: {os.path.basename(file_path)} - {error}")

    return successful, failed
