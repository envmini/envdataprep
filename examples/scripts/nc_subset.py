"""Example of extracting a subset of variables from a netCDF file."""

from pathlib import Path
import envdataprep as edp

# Set up input and output directories
ROOT = Path("F:/EnvMini/Examples")
input_dir = ROOT / "Input"
output_dir = ROOT / "Output"

# Select an example file
file_name = "S5P_RPRO_L2__NO2____20190101T233659_20190102T011828_06322_03_020400_20221106T093236.nc"
file_path = input_dir / file_name

# List all variables in the file
variables = edp.list_netcdf_variables(file_path)
print(*variables, sep='\n')

# List the variables to extract
variable_paths = [
    "PRODUCT/latitude",
    "PRODUCT/longitude",
    "PRODUCT/nitrogendioxide_tropospheric_column",
    "PRODUCT/SUPPORT_DATA/INPUT_DATA/cloud_fraction_crb",
]

# Extract and save out
# By default, the output file preserves the original group structure
edp.extract_and_write_netcdf(
    file_path,
    output_dir,
    variable_paths,
    output_name="example_output_original.nc",
    compression='zlib',
    compression_level=9,
)

# Rename the variables before save out
# By removing the "/" from the variable paths,
# The output file will have a flat structure
variable_renames = {
    "latitude": "PRODUCT/latitude",
    "longitude": "PRODUCT/longitude",
    "no2": "PRODUCT/nitrogendioxide_tropospheric_column",
    "cloud": "PRODUCT/SUPPORT_DATA/INPUT_DATA/cloud_fraction_crb",
}

edp.extract_and_write_netcdf(
    file_path,
    output_dir,
    variable_paths,
    output_name="example_output_renamed.nc",
    variable_renames=variable_renames,
    compression='zlib',
    compression_level=9,
)
