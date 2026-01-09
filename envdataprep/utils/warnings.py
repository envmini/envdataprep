"""Standardized warning system for envdataprep operations.

Environmental data comes from diverse sources with varying conventions and 
encoding practices. This module provides consistent, actionable warnings to 
help users validate data operations and handle edge cases across different 
formats (NetCDF, GRIB2, TIFF, etc.).
"""


def warn_unvalidated_subset(
    *,
    data_format: str,
    validate_subset: bool = False,
    warnings_enabled: bool = True,
):
    """
    Emit a standardized warning when subsetting is performed without
    validation.

    Parameters
    ----------
    data_format : str
        Name of the data format (e.g., 'netCDF', 'GRIB2', 'HDF').
    validate_subset : bool
        Whether automatic subset validation is enabled.
    warnings_enabled : bool
        Whether user-facing warnings are enabled.
    """
    if not warnings_enabled or validate_subset:
        return

    print(
        f"{data_format} data are provided by multiple sources with "
        "heterogeneous conventions, metadata completeness, and encoding "
        "practices.\n"
        "Subsetting is performed using standardized rules, but automatic "
        "validation cannot exhaustively cover all provider-specific edge "
        "cases.\n"
        "\n"
        "Recommendations:\n"
        " • Enable 'validate_subset=True' to perform built-in validation.\n"
        " • Perform an independent sanity check when working with a new "
        "   data provider or unfamiliar dataset.\n"
        " • Set 'warnings=False' to suppress this message."
    )
