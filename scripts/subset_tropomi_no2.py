"""Get subsets of TROPOMI NO2 files."""

import os
import glob

import envdataprep as edp


# Compression settings
COMPRESSION_METHOD = 'zlib'
COMPRESSION_LEVEL = 9

# List of target data fields from TROPOMI NO2 products
# For now, just keep all of the cloud fields that were used to retrieve NO2
# They do not take much space anyway
# This selection of data fields reduce the total files size by around 90%
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
    print(*files, sep="\n")

    for f in files:
        edp.extract_and_write_netcdf(
            f,
            output_dir,
            tropomi_no2_data_fields,
            compression=COMPRESSION_METHOD,
            compression_level=COMPRESSION_LEVEL,
        )
