"""Subsetting, comparison, and validation for netCDF files."""

import netCDF4 as nc
import numpy as np

from .read import list_netcdf_vars, extract_netcdf_as_dataset
from .write import rename_dataset_vars, write_netcdf

from ...utils.decorators import enable_parallel
from ...utils.io import build_subset_path
from ...utils.constants import DEFAULT_NETCDF_COMPLEVEL


# TODO: re-think about the logic of the @enable_parallel decorator
# one alternative way is to have a separate function for single file processing
# and a separate function for multi-file processing
# then wrap them under a single public api
# so the user sees the same interface
# the code will be more readable, but you will have more code to maintain

# Or, just go deeper into this decorator, understand the logic of the decorator
# and evaluate if this is the best way to handle multi-file processing
# for this entire package

@enable_parallel
def subset_netcdf(
    input_path: str,
    output_dir: str | None = None,
    keep_vars: list[str] | None = None,
    drop_vars: list[str] | None = None,
    output_name: str | None = None,
    use_input_name: bool = False,
    suffix: str = "_SUB",
    var_renames: dict[str, str] | None = None,
    compression: str | None = "zlib",
    complevel: int = DEFAULT_NETCDF_COMPLEVEL,
    shuffle: bool = True,
    fletcher32: bool = False,
    validate: bool = False,
    workers: int | None = None,
    show_progress: bool = True,
    **kwargs,
) -> None:
    """Extract or exclude variables from netCDF and write to new file.

    Parameters
    ----------
    input_path : str or list[str] or tuple[str, ...]
        Path(s) to input netCDF file(s).
    output_dir : str
        Output directory path.
    keep_vars : list[str], optional
        Variables to include in output (e.g., ["PRODUCT/latitude"]).
        Cannot be used together with drop_vars.
    drop_vars : list[str], optional
        Variables to exclude from output.
        Cannot be used together with keep_vars.
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
    workers : int, optional
        Number of parallel workers (multi-file only).
    show_progress : bool, default True
        Whether to show progress bar (multi-file only).
    **kwargs
        Additional arguments passed to Dataset.to_netcdf().

    Raises
    ------
    ValueError
        If both keep_vars and drop_vars are specified,
        if neither is specified, or if validation fails.
    """
    # Validate the input parameters
    if keep_vars is not None and drop_vars is not None:
        raise ValueError(
            "Cannot specify both 'keep_vars' and 'drop_vars'. "
            "Use one or the other."
        )

    if keep_vars is None and drop_vars is None:
        raise ValueError(
            "Either 'keep_vars' or 'drop_vars' must be specified."
        )

    # FIXME: this will cause confusion as well,
    # as list_netcdf_vars expects a single file path
    # List all variables in the input file
    all_vars = list_netcdf_vars(input_path)

    # Get the target variable paths for the subset
    # Initialize an empty list of variable paths
    # to avoid the linter warning
    var_paths = []

    if keep_vars is not None:
        var_paths = keep_vars
        missing = [v for v in keep_vars if v not in all_vars]
        if missing:
            raise ValueError(
                f"Variables not found in {input_path}: {missing}"
            )

    if drop_vars is not None:
        var_paths = [
            v for v in all_vars if v not in drop_vars
        ]
        missing = [v for v in drop_vars if v not in all_vars]
        if missing:
            raise ValueError(
                f"Variables not found in {input_path}: {missing}"
            )

    # Extract the specified variables as a dataset
    subset_ds = extract_netcdf_as_dataset(
        input_path=input_path,
        var_paths=var_paths,
    )

    # Compare the original and subset variables if validation is enabled
    if validate:
        with nc.Dataset(input_path, "r") as ds_a:
            # Compare global attributes
            if ds_a.attrs != subset_ds.attrs:
                raise ValueError(
                    "Subset global attributes do not match the original"
                )

            # Compare variables
            for var in var_paths:
                original_var = ds_a[var]
                subset_var = subset_ds[var]

                # Compare values (NaNs values are considered equal)
                if not np.array_equal(
                    original_var[:], subset_var[:], equal_nan=True,
                ):
                    raise ValueError(
                        f"Values for {var} do not match the original"
                    )

                # Compare attributes
                if original_var.attrs != subset_var.attrs:
                    raise ValueError(
                        f"Attributes for {var} do not match the original"
                    )

    # Rename the variables if specified
    if var_renames:
        subset_ds = rename_dataset_vars(
            dataset=subset_ds,
            var_renames=var_renames,
        )

    # Write the dataset to a new file
    output_path = build_subset_path(
        input_path=input_path,
        output_dir=output_dir,
        output_name=output_name,
        use_input_name=use_input_name,
        suffix=suffix,
    )

    write_netcdf(
        dataset=subset_ds,
        output_path=output_path,
        compression=compression,
        complevel=complevel,
        shuffle=shuffle,
        fletcher32=fletcher32,
        **kwargs,
    )
