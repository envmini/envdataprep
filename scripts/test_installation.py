"""Test whether envdataprep is properly installed."""

try:
    import envdataprep as edp
    print(f"envdataprep version {edp.__version__} is installed.")
except ImportError:
    print("envdataprep is not installed. Please see the README for details.")
