"""Example of extracting a subset of variables from a netCDF file."""

import os
import envdataprep as edp

# Set up input and output directories
ROOT = "D:\\Work\\Research\\Data\\Example_Data"
input_dir = os.path.join(ROOT, "Input")
output_dir = os.path.join(ROOT, "Output")

# Select an example file
file_name = "S5P_OFFL_L2__NO2____20250601T213842_20250601T232012_39562_03_020800_20250603T135817.nc"

# Target variables
target_variables = {
    "latitude": "PRODUCT/latitude",
    "longitude": "PRODUCT/longitude",
    "no2": "PRODUCT/nitrogendioxide_tropospheric_column",
    "cloud_frac": "PRODUCT/SUPPORT_DATA/INPUT_DATA/cloud_fraction_crb",
}


if __name__ == "__main__":
    file_path = os.path.join(input_dir, file_name)

    # Get all variables in the source file
    variables = edp.list_netcdf_variables(file_path)
    print(*variables, sep="\n")

    # Create a processor object to extract data from the source file
    processor = edp.NetCDFProcessor(
        file_path=file_path, target_variables=target_variables,
    )

    # Extract data from source file
    processor.extract_data()

    # Save out

    # 1) Flat structure, no compression, default output file name
    processor.to_netcdf(out_dir=output_dir)

    # 2) Flat structure, no compression, custom output file name
    processor.to_netcdf(
        out_dir=output_dir,
        output_name="flat_example_no_compression.nc"
    )

    # 3) Hierarchical structure, with compression, custom output file name
    processor.to_netcdf(
        out_dir=output_dir,
        output_name="hierarchical_example_compression.nc",
        preserve_groups=True,
        compression='zlib',
        compression_level=6
    )
