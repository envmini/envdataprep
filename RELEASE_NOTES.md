# EnvDataPrep Release Notes

## 🔧 v0.1.1 (Latest)

### Bug Fixes
- **Fixed import error**: Added `tqdm` to required dependencies
- **Improved installation**: No longer need `pip install envdataprep[parallel]`
- **Better user experience**: Package works immediately after `pip install envdataprep`

### Installation
```bash
pip install envdataprep
```

---

## 🎉 v0.1.0 (Initial Release)

First PyPI release securing the package name.

### Core Functionality
- **NetCDF Processing**: Complete netCDF file subsetting and variable extraction
- **Parallel Processing**: Multi-core processing for batch operations
- **Data Preservation**: Maintains original data integrity with configurable fill value handling

### Known Issues
- Requires `pip install envdataprep[parallel]` or separate `pip install tqdm` (fixed in v0.1.1)

### Key Features
- `list_netcdf_variables()` - Discover variables in netCDF files
- `subset_netcdf()` - Extract or exclude specific variables
- `extract_netcdf_as_xarray_dataset()` - Convert to xarray for analysis
- `process_files_parallel()` - Batch process multiple files

### Dependencies
- netCDF4 >= 1.6.0
- xarray >= 2023.1.0  
- numpy >= 1.24.0

## 🚀 Installation

```bash
pip install envdataprep
```

## 📖 Quick Start

```python
import envdataprep as edp

# List variables in a file
variables = edp.list_netcdf_variables('data.nc')

# Extract specific variables
edp.subset_netcdf(
    input_path='large_file.nc',
    output_dir='output/',
    include_variables=['temperature', 'pressure']
)
```

## 🔮 What's Next

This v0.1.0 release focuses on netCDF processing. Future releases will add:
- GRIB2 support
- GeoTIFF conversion
- Geographic clipping
- More format conversions

## 📝 Notes

- This is an alpha release focused on securing the PyPI package name
- Core netCDF functionality is stable and tested
- Additional features and improvements coming in future releases