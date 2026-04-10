"""Subsetting netCDF files."""

import os

from .read import list_netcdf_vars, extract_netcdf_as_dataset
from .write import rename_dataset_vars, write_netcdf
from ...utils.constants import DEFAULT_NETCDF_COMPLEVEL
from ...utils.io import build_subset_path
from ...utils.parallel import process_files_parallel


def _subset_netcdf_single(
    nc_input: str,
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
    **kwargs,
) -> None:
    """Subset one netCDF file and write the output to a new file."""

    if keep_vars is not None and drop_vars is not None:
        raise ValueError(
            "Cannot specify both 'keep_vars' and 'drop_vars'. "
            "Use one or the other."
        )

    all_vars = list_netcdf_vars(nc_input)

    if keep_vars is not None:
        var_paths = keep_vars
        missing = [v for v in keep_vars if v not in all_vars]
        if missing:
            raise ValueError(
                f"Variables not found in {nc_input}: {missing}"
            )
    elif drop_vars is not None:
        var_paths = [v for v in all_vars if v not in drop_vars]
        missing = [v for v in drop_vars if v not in all_vars]
        if missing:
            raise ValueError(
                f"Variables not found in {nc_input}: {missing}"
            )
    else:
        raise ValueError(
            "Either 'keep_vars' or 'drop_vars' must be specified."
        )

    subset_ds = extract_netcdf_as_dataset(
        input_path=nc_input,
        var_paths=var_paths,
    )

    if var_renames:
        subset_ds = rename_dataset_vars(
            dataset=subset_ds,
            var_renames=var_renames,
        )

    output_path = build_subset_path(
        input_path=nc_input,
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


def subset_netcdf(
    nc_input: str | list[str],
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
    workers: int | None = None,
    show_progress: bool = True,
    **kwargs,
):
    """Extract or exclude variables from netCDF and write to new file(s).

    Writes output file(s) and returns ``None``. Output paths follow
    :func:`~envdataprep.utils.io.build_subset_path` (same directory as input by
    default unless ``output_dir`` / ``output_name`` are set).

    Pass ``nc_input`` as one path string, or a list of paths. A list is
    processed **one file after another** in the current process when
    ``workers`` is ``None`` or ``1``. **Only** when ``workers`` is an integer
    ``> 1`` does processing use :class:`concurrent.futures.ProcessPoolExecutor`.
    For a single path string,
    ``workers`` and ``show_progress`` are ignored.

    Parameters
    ----------
    nc_input : str or list[str]
        Input netCDF path(s). List must be non-empty when used.
    output_dir : str, optional
        Output directory; default is each file's directory.
    keep_vars : list[str], optional
        Variables to include. Mutually exclusive with ``drop_vars``.
    drop_vars : list[str], optional
        Variables to exclude. Mutually exclusive with ``keep_vars``.
    output_name : str, optional
        Output filename; default derives from input with ``suffix``.
    use_input_name : bool, default True
        Passed to :func:`~envdataprep.utils.io.build_subset_path`.
    suffix : str, default "_SUB"
        Suffix for generated output names.
    var_renames : dict[str, str], optional
        Rename variables before write.
    compression : str or None, default 'zlib'
        Passed to :func:`~envdataprep.core.netcdf.write.write_netcdf`.
    complevel : int, default from constants
    shuffle, fletcher32
        Compression options for writing.
    workers : int, optional
        For a list of inputs only: if ``None`` or ``1``, run sequentially; if an
        integer ``> 1``, use that many worker processes.
    show_progress : bool, default True
        For a list with ``workers > 1`` only: tqdm over parallel tasks.
    **kwargs
        Extra arguments to :func:`~envdataprep.core.netcdf.write.write_netcdf`.

    Raises
    ------
    ValueError
        Invalid variable selection or validation failure.
    """
    subset_kw = {
        "output_dir": output_dir,
        "keep_vars": keep_vars,
        "drop_vars": drop_vars,
        "output_name": output_name,
        "use_input_name": use_input_name,
        "suffix": suffix,
        "var_renames": var_renames,
        "compression": compression,
        "complevel": complevel,
        "shuffle": shuffle,
        "fletcher32": fletcher32,
        **kwargs,
    }

    if isinstance(nc_input, str):
        _subset_netcdf_single(nc_input, **subset_kw)

    if isinstance(nc_input, list):
        # Sequential processing
        if workers is None or workers < 2:
            for file_path in nc_input:
                _subset_netcdf_single(file_path, **subset_kw)

        # Parallel processing
        if workers is not None and workers > 1:
            successful, failed = process_files_parallel(
                files=nc_input,
                process_func=_subset_netcdf_single,
                func_kwargs=subset_kw,
                max_workers=workers,
                show_progress=show_progress,
            )

            print(f"\nCompleted: {len(successful)} successful, {len(failed)} failed")

            if failed:
                print("\nFailed files:")
                for file_path, error in failed:
                    print(f"{os.path.basename(file_path)}: {error}")
