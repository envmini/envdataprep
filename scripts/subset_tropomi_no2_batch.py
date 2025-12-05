"""Get subsets of TROPOMI NO2 files using multiple cores."""

import os
import glob

import envdataprep as edp


# Compression settings
COMPRESSION_METHOD = 'zlib'
COMPRESSION_LEVEL = 9

# List of target data fields
tropomi_no2_data_fields = [
    'PRODUCT/latitude',
    'PRODUCT/longitude',
    'PRODUCT/time_utc',
    'PRODUCT/qa_value',
    'PRODUCT/nitrogendioxide_tropospheric_column',
    'PRODUCT/nitrogendioxide_tropospheric_column_precision',
    'PRODUCT/air_mass_factor_troposphere',
    'PRODUCT/SUPPORT_DATA/GEOLOCATIONS/solar_zenith_angle',
    'PRODUCT/SUPPORT_DATA/GEOLOCATIONS/viewing_zenith_angle',
    'PRODUCT/SUPPORT_DATA/GEOLOCATIONS/latitude_bounds',
    'PRODUCT/SUPPORT_DATA/GEOLOCATIONS/longitude_bounds',
    'PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/cloud_fraction_crb_nitrogendioxide_window',
    'PRODUCT/SUPPORT_DATA/DETAILED_RESULTS/cloud_radiance_fraction_nitrogendioxide_window',
    'PRODUCT/SUPPORT_DATA/INPUT_DATA/surface_altitude',
    'PRODUCT/SUPPORT_DATA/INPUT_DATA/eastward_wind',
    'PRODUCT/SUPPORT_DATA/INPUT_DATA/northward_wind',
    'PRODUCT/SUPPORT_DATA/INPUT_DATA/cloud_fraction_crb',
]


if __name__ == "__main__":
    input_dir = "D:\\Work\\Data\\Satellites\\TROPOMI\\OFFL\\NO2\\Countries\\CHN\\2019"
    output_dir = "D:\\Work\\Data\\Satellites\\TROPOMI\\OFFL\\NO2\\Countries\\CHN\\2019_SUB"

    files = sorted(glob.glob(os.path.join(input_dir, "*.nc")))
    print(f"Found {len(files)} files to process\n")

    # Process in parallel
    successful, failed = edp.process_files_parallel(
        files=files,
        process_func=edp.extract_and_write_netcdf,
        func_kwargs={
            "output_dir": output_dir,
            "variable_paths": tropomi_no2_data_fields,
            "compression": COMPRESSION_METHOD,
            "compression_level": COMPRESSION_LEVEL,
        },
        max_workers=18,  # Adjust based on your CPU
        show_progress=True,
    )

    print(f"\nCompleted: {len(successful)} successful, {len(failed)} failed")

    if failed:
        print("\nFailed files:")
        for file_path, error in failed:
            print(f"  {os.path.basename(file_path)}: {error}")
