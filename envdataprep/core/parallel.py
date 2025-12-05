# envdataprep/core/parallel.py
"""Parallel processing utilities for envdataprep."""

import os
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import Callable, List, Dict, Any, Optional, Tuple
from functools import partial


def _process_single_file(file_path, process_func, func_kwargs):
    """Wrapper to handle exceptions for single file processing."""
    try:
        result = process_func(file_path, **func_kwargs)
        return file_path, True, result, None
    except Exception as e:
        return file_path, False, None, str(e)


def process_files_parallel(
    files: List[str],
    process_func: Callable,
    func_kwargs: Optional[Dict[str, Any]] = None,
    max_workers: Optional[int] = None,
    show_progress: bool = True,
) -> Tuple[List[str], List[Tuple[str, str]]]:
    """Process multiple files in parallel.
    
    Parameters
    ----------
    files : List[str]
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
    Tuple[List[str], List[Tuple[str, str]]]
        (successful_files, failed_files_with_errors)
    
    Examples
    --------
    >>> from envdataprep.core.parallel import process_files_parallel
    >>> from envdataprep.core.io.netcdf import extract_and_write_netcdf
    >>> 
    >>> files = ["file1.nc", "file2.nc", "file3.nc"]
    >>> kwargs = {
    ...     "output_dir": "output/",
    ...     "variable_paths": ["PRODUCT/latitude", "PRODUCT/longitude"],
    ...     "compression": "zlib",
    ...     "compression_level": 9
    ... }
    >>> 
    >>> successful, failed = process_files_parallel(
    ...     files, extract_and_write_netcdf, func_kwargs=kwargs, max_workers=4
    ... )
    """
    func_kwargs = func_kwargs or {}
    max_workers = max_workers or os.cpu_count()
    
    successful = []
    failed = []
    
    # Use partial to bind arguments
    worker_func = partial(_process_single_file, process_func=process_func, func_kwargs=func_kwargs)
    
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(worker_func, f): f for f in files}
        
        if show_progress:
            try:
                from tqdm import tqdm
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


def _process_single_item(item, process_func, func_kwargs):
    """Wrapper to handle exceptions for single item processing."""
    try:
        result = process_func(item, **func_kwargs)
        return item, True, result, None
    except Exception as e:
        return item, False, None, str(e)


def batch_process(
    items: List[Any],
    process_func: Callable,
    func_kwargs: Optional[Dict[str, Any]] = None,
    max_workers: Optional[int] = None,
    show_progress: bool = True,
) -> Tuple[List[Any], List[Tuple[Any, str]]]:
    """Process a list of items in parallel (generic version).
    
    Parameters
    ----------
    items : List[Any]
        List of items to process.
    process_func : Callable
        Function to apply to each item. Must accept item as first argument.
    func_kwargs : Dict[str, Any], optional
        Additional keyword arguments to pass to process_func.
    max_workers : int, optional
        Number of parallel workers. If None, uses os.cpu_count().
    show_progress : bool, default True
        Whether to show progress bar (requires tqdm).
    
    Returns
    -------
    Tuple[List[Any], List[Tuple[Any, str]]]
        (successful_items, failed_items_with_errors)
    """
    func_kwargs = func_kwargs or {}
    max_workers = max_workers or os.cpu_count()
    
    successful = []
    failed = []
    
    # Use partial to bind arguments
    worker_func = partial(_process_single_item, process_func=process_func, func_kwargs=func_kwargs)
    
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(worker_func, item): item for item in items}
        
        if show_progress:
            try:
                from tqdm import tqdm
                iterator = tqdm(as_completed(futures), total=len(items), desc="Processing")
            except ImportError:
                print("Install tqdm for progress bar: pip install tqdm")
                iterator = as_completed(futures)
        else:
            iterator = as_completed(futures)
        
        for future in iterator:
            item, success, result, error = future.result()
            if success:
                successful.append(item)
            else:
                failed.append((item, error))
    
    return successful, failed
