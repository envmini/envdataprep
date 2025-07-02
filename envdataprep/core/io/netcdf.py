"""I/O functions and classes for netCDF files."""

import os
from collections import defaultdict
from typing import Dict, List, Optional, Union

import numpy as np
import xarray as xr
from netCDF4 import Dataset


def list_netcdf_variables(file_path: str) -> List[str]:
    """Get all available data variable paths in a netCDF file.

    Parameters
    ----------
    file_path : str
        Path to netCDF file.

    Returns
    -------
    List[str]
        List of variable paths.
    """
    with Dataset(file_path, 'r') as ds:
        return _collect_variable_paths(ds)


def _collect_variable_paths(
    group: Dataset, path_prefix: str = ""
) -> List[str]:
    """Recursively collect all variable paths from a netCDF group.

    This helper function traverses a netCDF group and its subgroups to collect
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
        if path_prefix:
            variable_path = f"{path_prefix}/{variable_name}"
        else:
            variable_path = variable_name
        variable_paths.append(variable_path)

    # Recursively get variables from subgroups
    for subgroup_name in group.groups:
        if path_prefix:
            subgroup_path = f"{path_prefix}/{subgroup_name}"
        else:
            subgroup_path = subgroup_name

        subgroup_vars = _collect_variable_paths(
            group.groups[subgroup_name], subgroup_path
        )
        variable_paths.extend(subgroup_vars)

    return variable_paths


def extract_netcdf_variable(
    file_path: str,
    variable_path: str,
    values_only: bool = False
) -> Union[xr.DataArray, np.ndarray]:
    """Extract a variable from a netCDF file using a path.

    Parameters
    ----------
    file_path : str
        Path to netCDF file.
    variable_path : str
        Path to variable (e.g., "group/variable" or "group/subgroup/variable").
    values_only : bool, default False
        If True, return the variable values instead of the DataArray.

    Returns
    -------
    Union[xr.DataArray, np.ndarray]
        The variable object or its values.
    """
    group_path, variable_name = variable_path.rsplit("/", 1)
    try:
        with xr.open_dataset(file_path, group=group_path) as ds:
            variable = ds[variable_name]
    except KeyError as e:
        raise KeyError(
            f"variable '{variable_path}' not found in dataset."
        ) from e

    if values_only:
        return variable.values

    return variable


class NetCDFProcessor:
    """Extract and process variables from netCDF files.

    This class provides functionality to extract specific variables from
    netCDF files and save them either as flat files or preserving the
    original group structure, with optional compression and custom naming.

    Parameters
    ----------
    file_path : str
        Path to the input netCDF file.
    target_variables : dict
        Mapping of {user_name: "group/subgroup/var"} specifying which
        variables to extract and their desired names in the output.

    Attributes
    ----------
    file_path : str
        Path to the input netCDF file.
    target_variables : dict
        Dictionary mapping user names to variable paths.
    extracted_dataset : xr.Dataset or None
        The extracted dataset after calling extract_data().
    """

    # Set compression algorithm mappings before initialization
    _COMPRESSION_CONFIGS = {
        'zlib': lambda level: {'zlib': True, 'complevel': level},
        'lzf': lambda level: {'compression': 'lzf'},
        'gzip': lambda level: {
            'compression': 'gzip',
            'compression_opts': level
        },
        'szip': lambda level: {'compression': 'szip'}
    }

    def __init__(self, file_path: str, target_variables: Dict[str, str]):
        """Initialize NetCDFProcessor."""
        self.file_path = file_path
        self.target_variables = target_variables
        self.extracted_dataset: Optional[xr.Dataset] = None

    def extract_data(self) -> xr.Dataset:
        """Extract requested variables from the netCDF file.

        Reads the specified variables from the input file and creates an
        in-memory xarray Dataset with additional metadata.

        Returns
        -------
        xr.Dataset
            Dataset containing the extracted variables with added metadata.

        Raises
        ------
        FileNotFoundError
            If the input file does not exist.
        KeyError
            If a requested variable path is not found in the file.
        """
        # Extract variables from the file
        data_arrays = {}
        for user_name, full_path in self.target_variables.items():
            data_arrays[user_name] = extract_netcdf_variable(
                self.file_path, full_path, values_only=False
            )

        # Create dataset with all variables
        self.extracted_dataset = xr.Dataset(data_arrays)

        # Add metadata to the dataset
        self.extracted_dataset.attrs.update({
            "source_file": os.path.basename(self.file_path),
            "processing_software": "envdataprep",
            "Conventions": "CF-1.8",
        })

        return self.extracted_dataset

    def _by_group(self) -> Dict[str, List[str]]:
        """Group variables by their netCDF group paths.

        Returns
        -------
        dict
            Mapping of {group_path: [user_var, ...]} where empty string
            represents the root group.
        """
        # Group variables by their netCDF group paths
        groups = defaultdict(list)
        for user_var, full_path in self.target_variables.items():
            if "/" in full_path:
                group_path = full_path.rsplit("/", 1)[0]
            else:
                group_path = ""
            groups[group_path].append(user_var)

        return groups

    def _group_dataset(self, vars_here: List[str]) -> xr.Dataset:
        """Build a mini-Dataset for one netCDF group.

        Strips duplicate dimension-coordinate variables
        from all child groups so they are not re-written
        in every subgroup.

        Parameters
        ----------
        vars_here : List[str]
            Variable names that belong to this group.

        Returns
        -------
        xr.Dataset
            Subset dataset with group-specific variables only.
        """
        try:
            sub_ds = self.extracted_dataset[vars_here]
        except TypeError as e:
            if self.extracted_dataset is None:
                raise RuntimeError(
                    "The extracted dataset is not available."
                ) from e
            raise

        # Remove non-dimension coordinates that don't belong here
        drop_coords = [
            c for c in sub_ds.coords
            if c not in vars_here and c not in sub_ds.dims
        ]
        if drop_coords:
            sub_ds = sub_ds.reset_coords(drop_coords, drop=True)

        # For child groups: strip the coordinate variables that
        # duplicate a dimension name
        # Determine the group path for this set of variables
        first_var_path = self.target_variables[vars_here[0]]

        if "/" in first_var_path:
            group_path = first_var_path.rsplit("/", 1)[0]
        else:
            group_path = ""

        if group_path:
            dims_to_strip = [
                d for d in sub_ds.dims if d in sub_ds.coords
            ]
            for dim in dims_to_strip:
                # Drop any pandas/xarray Index attached to the dim
                try:
                    sub_ds = sub_ds.drop_indexes(dim)
                except AttributeError:
                    # xarray < 0.20 – silently ignore, nothing to drop
                    pass
                # Remove the coordinate variable itself
                if dim in sub_ds.coords:
                    sub_ds = sub_ds.reset_coords(dim, drop=True)

        return sub_ds

    def _create_encoding(
        self,
        dataset: xr.Dataset,
        compression: Optional[str] = None,
        compression_level: int = 4,
        shuffle: bool = True,
        fletcher32: bool = False,
        chunksizes: Optional[Dict[str, Dict[str, int]]] = None
    ) -> Dict[str, Dict[str, Union[str, int, bool, tuple]]]:
        """Create encoding dictionary for netCDF output.

        Generates encoding settings for compression, chunking, and other
        netCDF4 features for all data variables in the dataset.

        Parameters
        ----------
        dataset : xr.Dataset
            Dataset to create encoding for.
        compression : str, optional
            Compression algorithm ('zlib', 'lzf', 'gzip', 'szip').
        compression_level : int, default 4
            Compression level (0-9 for applicable algorithms).
        shuffle : bool, default True
            Enable shuffle filter for better compression.
        fletcher32 : bool, default False
            Enable Fletcher32 checksum.
        chunksizes : dict, optional
            Custom chunk sizes as {var_name: {dim_name: size, ...}}.

        Returns
        -------
        Dict[str, Dict[str, Union[str, int, bool, tuple]]]
            Encoding dictionary for Dataset.to_netcdf().

        Raises
        ------
        ValueError
            If an unsupported compression algorithm is specified.
        """
        if compression is None:
            return {}

        if compression not in self._COMPRESSION_CONFIGS:
            valid_options = list(self._COMPRESSION_CONFIGS.keys())
            raise ValueError(
                f"Unsupported compression: {compression}. "
                f"Valid options: {valid_options}"
            )

        encoding = {}
        compression_config = self._COMPRESSION_CONFIGS[compression](
            compression_level
        )

        for var_name in dataset.data_vars:
            var_encoding = compression_config.copy()
            var_encoding.update({
                'shuffle': shuffle,
                'fletcher32': fletcher32
            })

            # Handle chunking
            if chunksizes and str(var_name) in chunksizes:
                var = dataset[var_name]
                var_chunksizes = chunksizes[str(var_name)]
                chunks = tuple(
                    var_chunksizes.get(str(dim), var.sizes[dim])
                    for dim in var.dims
                )
                var_encoding['chunksizes'] = chunks
            elif (hasattr(dataset[var_name], 'chunks') and
                  dataset[var_name].chunks is not None):
                var_chunks = dataset[var_name].chunks
                chunks = tuple(
                    c[0] if len(c) == 1 else max(c)
                    for c in var_chunks
                )
                var_encoding['chunksizes'] = chunks

            encoding[str(var_name)] = var_encoding

        return encoding

    def _generate_default_output_name(
        self,
        custom_name: Optional[str] = None
    ) -> str:
        """Generate default output filename with _SUB suffix.

        Parameters
        ----------
        custom_name : str, optional
            Custom filename to use instead of generating one.

        Returns
        -------
        str
            Output filename with _SUB suffix or the custom name.
        """
        if custom_name:
            return custom_name

        base_name = os.path.splitext(os.path.basename(self.file_path))[0]
        extension = os.path.splitext(self.file_path)[1]

        return f"{base_name}_SUB{extension}"

    def to_netcdf(
        self,
        output_name: Optional[str] = None,
        out_dir: Optional[str] = None,
        preserve_groups: bool = False,
        compression: Optional[str] = None,
        compression_level: int = 4,
        shuffle: bool = True,
        fletcher32: bool = False,
        chunksizes: Optional[Dict[str, Dict[str, int]]] = None,
        **flat_writer_kwargs,
    ) -> str:
        """Write extracted data to netCDF file with optional compression.

        Saves the extracted dataset either as a flat file (all variables in
        root group) or preserving the original group structure. Supports
        various compression algorithms and custom output naming.

        Parameters
        ----------
        output_name : str, optional
            Output filename. If None, generates default name with _SUB suffix.
        out_dir : str, optional
            Output directory. If None, uses current directory.
        preserve_groups : bool, default False
            If True, recreate original group/subgroup structure.
            If False, write all variables to root group (flat file).
        compression : str, optional
            Compression algorithm ('zlib', 'lzf', 'gzip', 'szip').
        compression_level : int, default 4
            Compression level (0-9). Higher values = better compression.
        shuffle : bool, default True
            Enable shuffle filter for better compression.
        fletcher32 : bool, default False
            Enable Fletcher32 checksum for error detection.
        chunksizes : dict, optional
            Custom chunk sizes as {var_name: {dim_name: size, ...}}.
        **flat_writer_kwargs
            Additional arguments passed to Dataset.to_netcdf() for flat files.

        Returns
        -------
        str
            Path to the created output file.

        Raises
        ------
        RuntimeError
            If extract_data() has not been called first.
        ValueError
            If an invalid compression algorithm is specified.
        OSError
            If output directory cannot be created or file cannot be written.
        """
        if self.extracted_dataset is None:
            raise RuntimeError("Call extract_data() before saving")

        # Generate output path
        if output_name is None:
            filename = self._generate_default_output_name()
        else:
            filename = output_name

        if out_dir is not None:
            os.makedirs(out_dir, exist_ok=True)
            output_path = os.path.join(out_dir, filename)
        else:
            output_path = filename

        # Write flat file
        if not preserve_groups:
            requested_vars = list(self.target_variables.keys())
            clean_dataset = self.extracted_dataset[requested_vars]

            # Drop non-dimension coordinates not requested
            drop_coords = [
                c for c in clean_dataset.coords
                if c not in requested_vars and c not in clean_dataset.dims
            ]
            if drop_coords:
                clean_dataset = clean_dataset.reset_coords(
                    drop_coords, drop=True
                )

            # Drop coordinate-variables that duplicate dims
            dims_to_strip = [
                d for d in clean_dataset.dims if d in clean_dataset.coords
            ]
            for dim in dims_to_strip:
                # Detach any Index object (xarray ≥ 0.20)
                try:
                    clean_dataset = clean_dataset.drop_indexes(dim)
                except AttributeError:
                    # xarray < 0.20 – silently ignore, nothing to drop
                    pass

                # Remove the coordinate variable itself
                if dim in clean_dataset.coords:
                    clean_dataset = clean_dataset.reset_coords(
                        dim, drop=True
                    )

            encoding = self._create_encoding(
                clean_dataset,
                compression=compression,
                compression_level=compression_level,
                shuffle=shuffle,
                fletcher32=fletcher32,
                chunksizes=chunksizes,
            )

            clean_dataset.to_netcdf(
                output_path,
                mode="w",
                format="NETCDF4",
                encoding=encoding,
                **flat_writer_kwargs,
            )
            return output_path

        # Write hierarchical file
        first_write = True
        for group_path, vars_here in self._by_group().items():
            sub_ds = self._group_dataset(vars_here)

            if group_path == "":
                sub_ds.attrs.update(self.extracted_dataset.attrs)

            encoding = self._create_encoding(
                sub_ds,
                compression=compression,
                compression_level=compression_level,
                shuffle=shuffle,
                fletcher32=fletcher32,
                chunksizes=chunksizes
            )

            sub_ds.to_netcdf(
                output_path,
                mode="w" if first_write else "a",
                format="NETCDF4",
                group=group_path if group_path else None,
                encoding=encoding,
            )
            first_write = False

        return output_path
