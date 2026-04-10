# EnvDataPrep release notes

## v0.1.2 (latest)

### Code changes
- Redesigned the code structure and simplified the public APIs.
- Fixed a bug in extracting netCDF metadata.

### Documentation
- **README** and **Examples** updated.

---
## v0.1.0 (initial PyPI)

First PyPI release to secure the package name.

### Highlights

- NetCDF variable listing and subsetting (`keep_vars` / `drop_vars`).
- Optional multi-file processing with `workers > 1`.
- Reads netCDF variables with automatic mask/scale **disabled** in the extraction path so stored values are preserved as written unless you change that in your own code.

### Quick start

```python
import envdataprep as edp

variables = edp.list_netcdf_vars("data.nc")

edp.subset_netcdf(
    nc_input="large_file.nc",
    output_dir="output/",
    keep_vars=["temperature", "pressure"],
)
```

---

## Roadmap (ideas)

- GRIB2 and GeoTIFF support  
- Geographic clipping and more format conversions  

---

## Notes

- **Alpha:** APIs may still change; pin versions in production if needed.
- **Verification:** Rely on your own checks for scientific correctness of subset outputs (variable lists, dtypes, mask/scale when reading files for comparison).
