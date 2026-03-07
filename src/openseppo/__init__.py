"""
openSEPPO — Open SEPPO Tools
Supporting Geospatial and Remote Sensing Data Processing

(c) 2026 Earth Big Data LLC  |  https://earthbigdata.com
Licensed under the Apache License, Version 2.0
https://github.com/EarthBigData/openSEPPO

Geospatial and SAR processing utilities. The tools are designed to scale
readily with the SEPPO (Scalable EO Processing Platform) software by
Earth Big Data (https://earthbigdata.com/seppo).

Subpackages
-----------
openseppo.nisar   — NISAR GCOV data search, download, and COG conversion
openseppo.cli     — Command-line entry points
"""

from importlib.metadata import version, PackageNotFoundError

try:
    __version__ = version("openseppo")
except PackageNotFoundError:
    __version__ = "0.0.0.dev0"

__all__ = ["__version__"]
