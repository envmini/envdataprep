"""
I/O functions and classes for netCDF files.

This module leverages two widely used Python libraries for working with
netCDF data: netCDF4 and Xarray. The design combines high-performance,
low-level file access with a simple and expressive interface.

The netCDF4 library is used primarily for reading netCDF files, as it
provides direct access to nested group hierarchies.

Xarray is used for writing netCDF files, benefiting from its concise
syntax and tight integration with labeled, multi-dimensional data
structures.

Utility functions are provided to convert between netCDF4 and xarray
objects where appropriate.
"""

# TODO: Improve the docstrings, comments and naming conventions throughout

import os
from collections import defaultdict
from typing import Dict, List, Optional, Tuple

import netCDF4 as nc
import numpy as np
import xarray as xr

from ..utils.io import (
    handle_file_errors,
    generate_subset_output_path,
    report_file_size_reduction,
)

from ..utils.warnings import warn_unvalidated_subset


# Based on experiments using the TROPOMI satellite product
# This is the optimal compression level
# Considering both reduced file size and processing time
# This was also recommended by AI
DEFAULT_NETCDF_COMPRESSION_LEVEL = 4


def _collect_netcdf_variable_paths(
    group: nc.Dataset, path_prefix: str = ""
) -> List[str]:
    """Recursively collect all variable paths from a netCDF group.

    This function traverses a netCDF group and its subgroups to collect
    all variable paths in a flat list. Paths use forward slashes as separators.

    Parameters
    ----------
    group : netCDF4.Dataset
        The netCDF group to traverse (can be root dataset or subgroup)
    path_prefix : str, default ""
        Current path prefix for building full variable paths.

    Returns
    -------
    List[str]
        List of all variable paths found in the group and its subgroups.
    """
    variable_paths = []

    # Get variables from the current group
    for variable_name in group.variables:
        full_path = f"{path_prefix}/{variable_name}" if path_prefix else variable_name
        variable_paths.append(full_path)

    # Recursively get variables from subgroups
    # The path_prefix is essential for hierarchical netCDF files with groups
    for subgroup_name in group.groups:
        subgroup_path = (
            f"{path_prefix}/{subgroup_name}" if path_prefix else subgroup_name
        )
        subgroup_variables = _collect_netcdf_variable_paths(
            group=group.groups[subgroup_name],
            path_prefix=subgroup_path,
        )
        variable_paths.extend(subgroup_variables)

    return variable_paths


@handle_file_errors(file_format="netCDF")
def list_netcdf_variables(file_path: str) -> List[str]:
    """Get all available variable paths in a netCDF file.

    Parameters
    ----------
    file_path : str
        Path to netCDF file.

    Returns
    -------
    List[str]
        List of variable paths.
    """
    with nc.Dataset(file_path, "r") as ds:
        return _collect_netcdf_variable_paths(group=ds)


def netcdf_variable_to_xarray_dataarray(
    nc_variable: nc.Variable,
    variable_path: str,
) -> xr.DataArray:
    """Convert a netCDF4 variable to an xarray DataArray.

    By default, this function preserves original fill values without
    converting them to NaNs, giving the user control over how to handle
    missing data representation.
    
    Parameters
    ----------
    nc_variable : netCDF4.Variable
        The netCDF4 variable to convert
    variable_path : str
        Path/name for the variable
    """

    # Get all attributes from the raw data
    attributes = {k: nc_variable.getncattr(k) for k in nc_variable.ncattrs()}

    # Important step before convertting netCDF4.Variable to xarray.DataArray

    # Turn off any automatic masking, scaling or other conversions
    # If there are attributes like "scale_factor" for a certain data field
    # Xarray will automatically alter its values without any warnings
    # Also, Xarray follows the CF conventions for netCDF files
    # It will automatically detect "_FillValue"
    # and convert filled values to NaNs
    # For netCDF files that do not follow the CF conventions
    # Xarray should not be able to automatically
    # convert filled values to NaNs

    # "set_auto_scale(False)" only disables
    # "scale_factor" and "add_offset"
    # but not the conversion of "_FillValue" to NaNs
    # To preserve the original data
    # use "set_auto_maskandscale(False)"
    nc_variable.set_auto_maskandscale(False)

    # Return the subset of the preserved raw data

    return xr.DataArray(
        data=nc_variable[:],
        dims=nc_variable.dimensions,
        attrs=attributes,
        name=variable_path,
    )


@handle_file_errors(file_format="netCDF")
def extract_netcdf_as_xarray_dataset(
    file_path: str, variable_paths: List[str]
) -> xr.Dataset:
    """Extract variables using netCDF4, return as flat xarray Dataset.

    Parameters
    ----------
    file_path : str
        Path to netCDF file.
    variable_paths : List[str]
        List of variable paths to extract (e.g., ["PRODUCT/latitude",]).
    rename_mapping : Dict[str, str], optional
        Mapping to rename variables {original_path: new_name}.

    Returns
    -------
    xr.Dataset
        Dataset containing extracted variables.
    """
    data_variables = {}

    with nc.Dataset(file_path, "r") as root_ds:
        for variable_path in variable_paths:
            try:
                nc_variable = root_ds[variable_path]
                data_array = netcdf_variable_to_xarray_dataarray(
                    nc_variable=nc_variable,
                    variable_path=variable_path,
                )
                data_variables[data_array.name] = data_array
            except KeyError as e:
                print(f"Warning: Variable '{variable_path}' not found: {e}")

        # Get global attributes
        global_attrs = {k: root_ds.getncattr(k) for k in root_ds.ncattrs()}

    xarray_dataset = xr.Dataset(data_variables, attrs=global_attrs)
    return xarray_dataset


# TODO: Does this work for xr.DataArrays as well?
def rename_xarray_dataset_variables(
    xarray_dataset: xr.Dataset, variable_renames: Dict[str, str]
) -> xr.Dataset:
    """Rename variables in an xarray dataset.

    Parameters
    ----------
    dataset : xr.Dataset
        Dataset to rename variables in.
    variable_renames : Dict[str, str]
        Mapping of {old_name: new_name} for renaming variables.

    Examples
    --------
    >>> renames = {"PRODUCT/latitude": "lat", "PRODUCT/longitude": "lon"}
    >>> renamed_ds = rename_xarray_dataset_variables(dataset, renames)
    """
    for old_name, new_name in variable_renames.items():
        xarray_dataset = xarray_dataset.rename({old_name: new_name})
    return xarray_dataset


def _create_group_mapping(variable_names: List[str]) -> Dict[str, List[str]]:
    """Create mapping of group paths to variable names for hierarchical writing.

    Parameters
    ----------
    variable_names : List[str]
        Variable names (already renamed if needed). Can contain "/" for grouping.

    Returns
    -------
    Dict[str, List[str]]
        Mapping of {group_path: [variable_names, ...]}.
        Empty string key represents root group.
    """
    groups = defaultdict(list)

    for variable_name in variable_names:
        if "/" in variable_name:
            group_path, _ = variable_name.rsplit("/", 1)
        else:
            group_path = ""  # Root group

        groups[group_path].append(variable_name)

    return dict(groups)


def _clean_group_variable_names(
    dataset: xr.Dataset, group_path: str
) -> xr.Dataset:
    """Remove group prefix from variable names for netCDF group writing.

    Transforms "PRODUCT/latitude" → "latitude" when writing to PRODUCT group.

    Parameters
    ----------
    dataset : xr.Dataset
        Dataset with potentially prefixed variable names.
    group_path : str
        Group path prefix to remove (e.g., "PRODUCT").

    Returns
    -------
    xr.Dataset
        Dataset with cleaned variable names.
    """
    if not group_path:
        return dataset

    renamed_vars = {}
    prefix = f"{group_path}/"

    for name, var in dataset.data_vars.items():
        if name.startswith(prefix):
            clean_name = name[len(prefix):]
        else:
            clean_name = name
        renamed_vars[clean_name] = var

    return xr.Dataset(
        renamed_vars,
        coords=dataset.coords,
        attrs=dataset.attrs,
    )


def _create_encoding(
    xarray_dataset: xr.Dataset,
    compression_method: Optional[str] = "zlib",  # Changed default back to zlib
    compression_level: int = DEFAULT_NETCDF_COMPRESSION_LEVEL,
    shuffle: bool = True,
    fletcher32: bool = False,  # Changed default to False
) -> Dict:
    """Create encoding dictionary for netCDF output.
    
    Only compresses numeric variables larger than 1KB to avoid encoding conflicts.
    """
    if compression_method is None:
        return {}

    # TODO: consider whether to force compression via zlib
    # Or, what about try to use zlib by default
    # And when it does not work for some data source, fall back to no compression
    # Currently only stick with zlib which works
    # Other algorithms (gzip, szip, lzf) lead to errors
    # And the trials show that zlib significantly accelerates the writing process
    # Should conduct more tests on this
    compression_configs = {
        "zlib": lambda level: {"zlib": True, "complevel": level},
    }

    if compression_method not in compression_configs:
        valid_options = list(compression_configs.keys())
        raise ValueError(
            f"Unsupported compression: {compression_method}. "
            f"Valid options: {valid_options}"
        )

    encoding = {}
    compression_config = compression_configs[compression_method](compression_level)

    for variable_name in xarray_dataset.data_vars:
        var = xarray_dataset[variable_name]

        # Only compress numeric variables larger than 1KB
        if (var.dtype.kind in ['f', 'i', 'u'] and  # float, int, uint only
            var.nbytes > 1024):  # Skip tiny variables

            variable_encoding = compression_config.copy()
            variable_encoding.update({
                "shuffle": shuffle,
                "fletcher32": fletcher32
            })
            encoding[str(variable_name)] = variable_encoding

        # For all other variables (strings, small arrays, etc.),
        # use empty encoding - let netCDF4 handle them safely

    return encoding


# TODO: Does this work for xr.DataArray as well?
# Or, maybe no need to allow that
def write_netcdf(
    xarray_dataset: xr.Dataset,
    output_path: str,
    compression_method: Optional[str] = "zlib",
    compression_level: int = DEFAULT_NETCDF_COMPRESSION_LEVEL,
    shuffle: bool = True,
    fletcher32: bool = False,
    **kwargs,
) -> None:
    """Write xarray Dataset to netCDF file with compression and group structure.

    Parameters
    ----------
    xarray_dataset : xr.Dataset
        Dataset to write. Variable names with "/" will create groups.
    output_path : str
        Full path for output file (including filename).
    compression_method : str, optional, default 'zlib'
        Compression algorithm ('zlib', None).
    compression_level : int, default 9
        Compression level (0-9).
    shuffle : bool, default True
        Enable shuffle filter for better compression.
    fletcher32 : bool, default False
        Enable Fletcher32 checksum for error detection.
    **kwargs
        Additional arguments passed to Dataset.to_netcdf().

    Returns
    -------
    str
        Path to the created output file.
    """
    # Create output directory if it doesn't exist
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    # Create group mapping from variable names
    variable_names = list(xarray_dataset.data_vars.keys())
    group_mapping = _create_group_mapping(variable_names)

    # Start writing
    first_write = True

    for group_path, variable_names in group_mapping.items():
        # Get subset of variables for this group
        group_variables = {
            name: xarray_dataset[name]
            for name in variable_names
            if name in xarray_dataset
        }

        if not group_variables:
            continue

        sub_dataset = xr.Dataset(
            group_variables,
            coords=xarray_dataset.coords,
            attrs=xarray_dataset.attrs,
        )

        # Clean variable names for this group (remove group prefix)
        sub_dataset = _clean_group_variable_names(sub_dataset, group_path)

        encoding = _create_encoding(
            xarray_dataset=sub_dataset,
            compression_method=compression_method,
            compression_level=compression_level,
            shuffle=shuffle,
            fletcher32=fletcher32,
        )

        sub_dataset.to_netcdf(
            output_path,
            mode="w" if first_write else "a",
            group=group_path if group_path else None,
            encoding=encoding,
            **kwargs,
        )

        # After writing out the first sub_dataset
        # The following writing should use the Append mode
        first_write = False


def verify_netcdf_file():
    raise NotImplementedError("verify_netcdf_file is not implemented yet.")


# FIXME: This may not handle the NaNs well
# Do not blindly follow AI
# Learn from this example and think about what you actually need

# import numpy as np
# var = 'nitrogendioxide_tropospheric_column'
# a = ds1[var].values
# b = ds2[var].values
# np.array_equal(a, b, equal_nan=True)


def compare_netcdf_variables(
    file_a_path: str,
    file_b_path: str,
    variable_paths: Optional[List[str]] = None,
    exact_match: bool = True,
    tolerance: float = 1e-10,
    compare_attributes: bool = True
) -> Dict[str, Dict[str, bool]]:
    """Compare shared variables between two netCDF files.
    
    Parameters
    ----------
    file_a_path : str
        Path to first netCDF file.
    file_b_path : str
        Path to second netCDF file.
    variable_paths : List[str], optional
        Specific variables to compare. If None, compares all shared variables.
    exact_match : bool, default True
        If True, requires exact binary equality (no tolerance).
        If False, uses numerical tolerance for floating point comparisons.
    tolerance : float, default 1e-10
        Numerical tolerance (only used if exact_match=False).
    compare_attributes : bool, default True
        Whether to compare variable attributes.
        
    Returns
    -------
    Dict[str, Dict[str, bool]]
        Results for each variable: 
        {
            "PRODUCT/latitude": {"data_match": True, "attributes_match": True},
            "PRODUCT/longitude": {"data_match": False, "attributes_match": True}
        }
        
    Examples
    --------
    >>> # Compare subset against original (exact match)
    >>> results = compare_netcdf_variables("original.nc", "subset.nc")
    >>> 
    >>> # Compare two different files with tolerance
    >>> results = compare_netcdf_variables(
    ...     "file1.nc", "file2.nc", exact_match=False, tolerance=1e-6
    ... )
    """
    results = {}

    with nc.Dataset(file_a_path, "r") as ds_a, nc.Dataset(file_b_path, "r") as ds_b:

        # Determine which variables to compare
        if variable_paths is None:
            # If the variables are not specified
            # Find all shared variables
            variables_from_a = set(_collect_netcdf_variable_paths(ds_a))
            variables_from_b = set(_collect_netcdf_variable_paths(ds_b))
            variable_paths = list(variables_from_a.intersection(variables_from_b))

        for variable_path in variable_paths:
            variable_results = {"data_match": False, "attributes_match": False}

            try:
                variable_a = ds_a[variable_path]
                variable_b = ds_b[variable_path]

                # Compare data - exact or with tolerance
                if exact_match:
                    variable_results["data_match"] = np.array_equal(variable_a[:], variable_b[:])
                else:
                    try:
                        variable_results["data_match"] = np.allclose(
                            variable_a[:],
                            variable_b[:],
                            rtol=tolerance,
                            atol=tolerance,
                            equal_nan=True,
                        )
                    except (ValueError, TypeError):
                        # Fallback to exact for non-numeric data
                        variable_results["data_match"] = np.array_equal(
                            variable_a[:], variable_b[:], equal_nan=True
                        )

                # Compare attributes if requested
                if compare_attributes:
                    attrs_a = {k: variable_a.getncattr(k) for k in variable_a.ncattrs()}
                    attrs_b = {k: variable_b.getncattr(k) for k in variable_b.ncattrs()}
                    variable_results["attributes_match"] = attrs_a == attrs_b
                else:
                    variable_results["attributes_match"] = True

            except KeyError as e:
                print(f"Warning: Variable '{variable_path}' not found in one of the files: {e}")
                continue

            results[variable_path] = variable_results

    return results


def validate_netcdf_subset(
    original_path: str,
    subset_path: str,
    variable_paths: Optional[List[str]] = None,
    exact_match: bool = True,
    tolerance: float = 1e-10
) -> bool:
    """Validate that netCDF subset preserves data values exactly.
    
    This is a convenience wrapper around compare_netcdf_variables for subset validation.
    
    Parameters
    ----------
    original_path : str
        Path to original netCDF file.
    subset_path : str
        Path to subset netCDF file.
    variable_paths : List[str]
        Variables that were extracted in the subset.
    exact_match : bool, default True
        If True, requires exact binary equality.
    tolerance : float, default 1e-10
        Numerical tolerance (only used if exact_match=False).
        
    Returns
    -------
    bool
        True if all variables match exactly, False otherwise.
        
    Raises
    ------
    ValueError
        If any variables don't match, with details about differences.
        
    Examples
    --------
    >>> # Validate exact subset
    >>> validate_netcdf_subset(
    ...     "original.nc", "subset.nc", 
    ...     ["PRODUCT/latitude", "PRODUCT/longitude"]
    ... )
    True
    """
    results = compare_netcdf_variables(
        file_a_path=original_path,
        file_b_path=subset_path,
        variable_paths=variable_paths,
        exact_match=exact_match,
        tolerance=tolerance,
    )

    failed_variables = []
    for variable_path, variable_results in results.items():
        if not all(variable_results.values()):
            failed_variables.append(variable_path)

    if failed_variables:
        raise ValueError(f"Validation failed for variables: {failed_variables}")

    return True


def subset_netcdf(
    input_path: str,
    output_dir: str,
    include_variables: Optional[List[str]] = None,
    drop_variables: Optional[List[str]] = None,
    output_name: Optional[str] = None,
    variable_renames: Optional[Dict[str, str]] = None,
    compression_method: Optional[str] = "zlib",
    compression_level: int = DEFAULT_NETCDF_COMPRESSION_LEVEL,
    shuffle: bool = True,
    fletcher32: bool = False,
    show_size_reduction: bool = False,
    warnings_enabled: bool = True,
    validate_subset: bool = False,
    delete_original: bool = False,
    **kwargs,
) -> None:
    """Extract or exclude variables from netCDF and write to new file in one step.

    Parameters
    ----------
    input_path : str
        Path to input netCDF file.
    output_dir : str
        Output directory path.
    include_variables : List[str], optional
        Variables to include in output (e.g., ["PRODUCT/latitude", "PRODUCT/longitude"]).
        Cannot be used together with drop_variables.
    drop_variables : List[str], optional
        Variables to exclude from output.
        Cannot be used together with include_variables.
    output_name : str, optional
        Output filename. If None, generates from input filename with _SUB suffix.
    variable_renames : Dict[str, str], optional
        Rename variables {old_name: new_name}.
    compression_method : str, optional, default 'zlib'
        Compression algorithm ('zlib', 'lzf', 'gzip', 'szip', None).
    compression_level : int, default 4
        Compression level (0-9).
    shuffle : bool, default True
        Enable shuffle filter for better compression.
    fletcher32 : bool, default False
        Enable Fletcher32 checksum for error detection.
    warnings : bool, default True
        Whether to print validation warnings and recommendations.
    validate_subset : bool, default False
        Whether to automatically validate the output subset against original.
    delete_original : bool, default False
        Whether to delete the original file after successful subsetting.
    **kwargs
        Additional arguments passed to Dataset.to_netcdf().

    Returns
    -------
    str
        Path to created output file.
        
    Raises
    ------
    ValueError
        If both include_variables and drop_variables are specified.
        
    Examples
    --------
    >>> # Include only specific variables
    >>> subset_netcdf("data.nc", "output/", 
    ...               include_variables=["temperature", "pressure"])
    >>> 
    >>> # Exclude specific variables (keep everything else)
    >>> subset_netcdf("data.nc", "output/",
    ...               drop_variables=["qa_flags", "processing_metadata"])
    """
    # Validate mutually exclusive parameters
    if include_variables is not None and drop_variables is not None:
        raise ValueError(
            "Cannot specify both 'include_variables' and 'drop_variables'."
            "Use one or the other."
        )

    # Get all the variables in the file
    all_variables = list_netcdf_variables(input_path)

    # Determine which variables to extract
    if drop_variables is not None:
        # Get all variables, then exclude the ones to drop
        variable_paths = [var for var in all_variables if var not in drop_variables]

        if warnings_enabled and len(variable_paths) == len(all_variables):
            print(f"Warning: None of the drop_variables {drop_variables} were found in the file")

    elif include_variables is not None:
        variable_paths = include_variables
        # Ensure all specified variables exist in the file
        missing_variables = [var for var in include_variables if var not in all_variables]

        # FIXME: This may be too hard, sometimes, the subsets may not be the same?
        # Make this more flexible? Maybe, don't break the programme?
        if missing_variables:
            raise ValueError(
                f"The following variables were not found in the file: {missing_variables}"
                )

    else:
        # If neither specified, raise an error
        raise ValueError(
            "Either 'include_variables' or 'drop_variables' must be specified."
        )

    # Extract variables from netCDF
    xarray_dataset = extract_netcdf_as_xarray_dataset(
        file_path=input_path,
        variable_paths=variable_paths,
    )

    # Rename variables if needed
    if variable_renames:
        xarray_dataset = rename_xarray_dataset_variables(
            xarray_dataset=xarray_dataset,
            variable_renames=variable_renames,
        )

    # Generate output path
    output_path = generate_subset_output_path(
        input_path=input_path,
        output_dir=output_dir,
        custom_name=output_name,
    )

    # Write to netCDF file
    write_netcdf(
        xarray_dataset=xarray_dataset,
        output_path=output_path,
        compression_method=compression_method,
        compression_level=compression_level,
        shuffle=shuffle,
        fletcher32=fletcher32,
        **kwargs,
    )

    # For now, assume that it works well with TROPOMI

    # If requested, show the file size reduction
    # if show_size_reduction:
    #     report_file_size_reduction(
    #         input_path=input_path,
    #         output_path=output_path,
    #     )

    # # By default, print warnings to encourage validation
    # # This can be turned off by the user
    # warn_unvalidated_subset(
    #     data_format="netCDF",
    #     validate_subset=validate_subset,
    #     warnings_enabled=warnings_enabled,
    # )

    # # If requested, validate the output
    # if validate_subset:
    #     try:
    #         print("🔍 Validating subset...")
    #         is_valid = validate_netcdf_subset(
    #             original_path=input_path,
    #             subset_path=output_path,
    #             variable_paths=variable_paths,
    #             exact_match=True
    #         )
    #         if is_valid:
    #             print("✅ Validation passed: Subset data matches original exactly")
    #         else:
    #             print("❌ Validation failed: Subset data differs from original")
    #     except Exception as e:
    #         print(f"⚠️  Validation error: {e}")
    #         print("   Manual verification recommended")

    # # If requested, delete the original file
    # if delete_original:
    #     try:
    #         os.remove(input_path)
    #         if warnings:
    #             print(f"🗑️  Original file deleted: {input_path}")
    #     except Exception as e:
    #         print(f"⚠️  Could not delete original file: {e}")

    # return output_path


def split_netcdf_by_dim():
    raise NotImplementedError("split_netcdf is not implemented yet.")

# Do what is not provided by Xarray
def merge_netcdf_by_dim():
    raise NotImplementedError("merge_netcdf is not implemented yet.")


# The function should work with multiple shape formats
def clip_netcdf_to_shape(
    input_path: str,
    shape_path: str,  # GeoJSON, Shapefile, etc.
    output_path: str,
    variable_paths: Optional[List[str]] = None,
    **kwargs
) -> str:
    """Clip NetCDF data to geographic shape boundary.
    
    Extracts only data points that fall within the provided shape,
    significantly reducing file size for regional studies.
    
    Parameters
    ----------
    input_path : str
        Path to input NetCDF file.
    shape_path : str
        Path to shape file (GeoJSON, Shapefile, etc.).
    output_path : str
        Path for clipped output file.
    variable_paths : List[str], optional
        Variables to include. If None, includes all variables.
    """
    raise NotImplementedError("clip_netcdf_to_shape is not implemented yet.")


def clip_netcdf_to_bbox(
    input_path: str,
    bbox: Tuple[float, float, float, float],  # (min_lon, min_lat, max_lon, max_lat)
    output_path: str,
    variable_paths: Optional[List[str]] = None,
    **kwargs
) -> str:
    """Clip NetCDF data to bounding box.
    
    Simpler alternative to shape clipping for rectangular regions.
    """
    raise NotImplementedError("clip_netcdf_to_bbox is not implemented yet.")


def sample_netcdf_at_points(
    input_path: str,
    points: List[Tuple[float, float]],  # [(lon, lat), ...]
    output_path: str,
    variable_paths: Optional[List[str]] = None,
    **kwargs
) -> str:
    """Extract NetCDF data at specific point locations.
    
    Useful for validation against ground stations or creating
    time series at specific locations.
    """
    raise NotImplementedError("sample_netcdf_at_points is not implemented yet.")
