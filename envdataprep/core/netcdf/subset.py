"""Subsetting, comparison, and validation for netCDF files."""

import warnings

import netCDF4 as nc
import numpy as np

from ...utils.decorators import enable_parallel
from ...utils.io import build_output_path
from ._helpers import _collect_netcdf_var_paths, DEFAULT_NETCDF_COMPLEVEL
from .read import list_netcdf_vars, extract_netcdf_as_dataset
from .write import rename_dataset_vars, write_netcdf


# FIXME: May not handle NaNs well in all cases
# np.array_equal does not treat NaNs as equal by default


def compare_netcdf_vars(
    input_path_a: str,
    input_path_b: str,
    var_paths: list[str] | None = None,
    exact_match: bool = True,
    tolerance: float = 1e-10,
    compare_attrs: bool = True,
) -> dict[str, dict[str, bool]]:
    """Compare shared variables between two netCDF files.

    Parameters
    ----------
    input_path_a : str
        Path to first netCDF file.
    input_path_b : str
        Path to second netCDF file.
    var_paths : list[str], optional
        Specific variables to compare. If None, compares all shared variables.
    exact_match : bool, default True
        If True, requires exact binary equality (no tolerance).
        If False, uses numerical tolerance for floating point comparisons.
    tolerance : float, default 1e-10
        Numerical tolerance (only used if exact_match=False).
    compare_attrs : bool, default True
        Whether to compare variable attributes.

    Returns
    -------
    dict[str, dict[str, bool]]
        Results for each variable:
        {"PRODUCT/latitude": {"data_match": True, "attrs_match": True}, ...}

    Examples
    --------
    >>> results = compare_netcdf_vars("original.nc", "subset.nc")
    >>>
    >>> results = compare_netcdf_vars(
    ...     "file1.nc", "file2.nc", exact_match=False, tolerance=1e-6
    ... )
    """
    results = {}

    with nc.Dataset(input_path_a, "r") as ds_a, nc.Dataset(input_path_b, "r") as ds_b:
        if var_paths is None:
            vars_a = set(_collect_netcdf_var_paths(ds_a))
            vars_b = set(_collect_netcdf_var_paths(ds_b))
            var_paths = list(vars_a & vars_b)

        for var_path in var_paths:
            var_result = {"data_match": False, "attrs_match": False}

            try:
                var_a = ds_a[var_path]
                var_b = ds_b[var_path]

                if exact_match:
                    var_result["data_match"] = np.array_equal(
                        var_a[:], var_b[:]
                    )
                else:
                    try:
                        var_result["data_match"] = np.allclose(
                            var_a[:], var_b[:],
                            rtol=tolerance, atol=tolerance,
                            equal_nan=True,
                        )
                    except (ValueError, TypeError):
                        var_result["data_match"] = np.array_equal(
                            var_a[:], var_b[:], equal_nan=True
                        )

                if compare_attrs:
                    attrs_a = {k: var_a.getncattr(k) for k in var_a.ncattrs()}
                    attrs_b = {k: var_b.getncattr(k) for k in var_b.ncattrs()}
                    var_result["attrs_match"] = attrs_a == attrs_b
                else:
                    var_result["attrs_match"] = True

            except KeyError:
                warnings.warn(
                    f"Variable '{var_path}' not found in one of the files"
                )
                continue

            results[var_path] = var_result

    return results


def validate_netcdf_subset(
    original_path: str,
    subset_path: str,
    var_paths: list[str] | None = None,
    exact_match: bool = True,
    tolerance: float = 1e-10,
) -> bool:
    """Validate that netCDF subset preserves data values exactly.

    Convenience wrapper around compare_netcdf_vars for subset validation.

    Parameters
    ----------
    original_path : str
        Path to original netCDF file.
    subset_path : str
        Path to subset netCDF file.
    var_paths : list[str], optional
        Variables that were extracted in the subset.
    exact_match : bool, default True
        If True, requires exact binary equality.
    tolerance : float, default 1e-10
        Numerical tolerance (only used if exact_match=False).

    Returns
    -------
    bool
        True if all variables match exactly.

    Raises
    ------
    ValueError
        If any variables don't match.

    Examples
    --------
    >>> validate_netcdf_subset(
    ...     "original.nc", "subset.nc",
    ...     ["PRODUCT/latitude", "PRODUCT/longitude"]
    ... )
    True
    """
    results = compare_netcdf_vars(
        input_path_a=original_path,
        input_path_b=subset_path,
        var_paths=var_paths,
        exact_match=exact_match,
        tolerance=tolerance,
    )

    failed = [vp for vp, vr in results.items() if not all(vr.values())]
    if failed:
        raise ValueError(f"Validation failed for variables: {failed}")

    return True


@enable_parallel
def subset_netcdf(
    input_path: str,
    output_dir: str,
    include_vars: list[str] | None = None,
    drop_vars: list[str] | None = None,
    output_name: str | None = None,
    var_renames: dict[str, str] | None = None,
    compression: str | None = "zlib",
    complevel: int = DEFAULT_NETCDF_COMPLEVEL,
    shuffle: bool = True,
    fletcher32: bool = False,
    validate: bool = False,
    warnings_enabled: bool = True,
    workers: int | None = None,
    show_progress: bool = True,
    **kwargs,
) -> str:
    """Extract or exclude variables from netCDF and write to new file.

    Parameters
    ----------
    input_path : str or list[str]
        Path(s) to input netCDF file(s).
    output_dir : str
        Output directory path.
    include_vars : list[str], optional
        Variables to include in output (e.g., ["PRODUCT/latitude"]).
        Cannot be used together with drop_vars.
    drop_vars : list[str], optional
        Variables to exclude from output.
        Cannot be used together with include_vars.
    output_name : str, optional
        Output filename. If None, generates from input filename with _SUB
        suffix. Should be left as None when processing multiple files.
    var_renames : dict[str, str], optional
        Rename variables {old_name: new_name}.
    compression : str or None, default 'zlib'
        Compression algorithm. Currently only 'zlib' is supported.
        Pass None to disable compression.
    complevel : int, default 4
        Compression level (0-9).
    shuffle : bool, default True
        Enable shuffle filter for better compression.
    fletcher32 : bool, default False
        Enable Fletcher32 checksum for error detection.
    validate : bool, default False
        If True, automatically verify that the output file preserves
        the original data values after writing. Raises ValueError
        on mismatch.
    warnings_enabled : bool, default True
        Whether to emit user-facing warnings.
    workers : int, optional
        Number of parallel workers (multi-file only).
    show_progress : bool, default True
        Whether to show progress bar (multi-file only).
    **kwargs
        Additional arguments passed to Dataset.to_netcdf().

    Returns
    -------
    str
        Path to created output file (single-file).
    tuple[list[str], list[tuple[str, str]]]
        (successful_paths, failed_with_errors) when input_path is a list.

    Raises
    ------
    ValueError
        If both include_vars and drop_vars are specified,
        if neither is specified, or if validation fails.

    Examples
    --------
    >>> # Single file
    >>> subset_netcdf("data.nc", "output/",
    ...               include_vars=["temperature", "pressure"])
    >>>
    >>> # With automatic validation
    >>> subset_netcdf("data.nc", "output/",
    ...               include_vars=["temperature"], validate=True)
    >>>
    >>> # Multiple files in parallel
    >>> subset_netcdf(["a.nc", "b.nc", "c.nc"], "output/",
    ...               include_vars=["temperature"], workers=4)
    """
    if include_vars is not None and drop_vars is not None:
        raise ValueError(
            "Cannot specify both 'include_vars' and 'drop_vars'. "
            "Use one or the other."
        )

    all_vars = list_netcdf_vars(input_path)

    if drop_vars is not None:
        var_paths = [v for v in all_vars if v not in drop_vars]
        if warnings_enabled and len(var_paths) == len(all_vars):
            warnings.warn(
                f"None of the drop_vars {drop_vars} "
                f"were found in {input_path}"
            )

    elif include_vars is not None:
        var_paths = include_vars
        missing = [v for v in include_vars if v not in all_vars]
        if missing:
            raise ValueError(
                f"Variables not found in {input_path}: {missing}"
            )

    else:
        raise ValueError(
            "Either 'include_vars' or 'drop_vars' must be specified."
        )

    dataset = extract_netcdf_as_dataset(
        input_path=input_path,
        var_paths=var_paths,
    )

    if var_renames:
        dataset = rename_dataset_vars(
            dataset=dataset,
            var_renames=var_renames,
        )

    output_path = build_output_path(
        input_path=input_path,
        output_dir=output_dir,
        custom_name=output_name,
        suffix="_SUB",
    )

    write_netcdf(
        dataset=dataset,
        output_path=output_path,
        compression=compression,
        complevel=complevel,
        shuffle=shuffle,
        fletcher32=fletcher32,
        **kwargs,
    )

    if validate:
        validate_netcdf_subset(
            original_path=input_path,
            subset_path=output_path,
            var_paths=var_paths,
        )

    return output_path
