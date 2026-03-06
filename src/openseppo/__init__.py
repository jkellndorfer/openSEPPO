"""
openSEPPO — Open SEPPO Tools

Geospatial and SAR processing utilities organized into subpackages mirroring
the SEPPO module structure:

    openseppo.core          — core utilities (I/O, naming, datetime helpers)
    openseppo.geospatial    — raster, vector, sensors, tsa, tools
    openseppo.cloud         — cloud storage (AWS S3, Wasabi)
    openseppo.parsers       — file/metadata parsers
    openseppo.ui            — user-interaction helpers
    openseppo.cli           — command-line entry points
"""

from importlib.metadata import version, PackageNotFoundError

try:
    __version__ = version("openseppo")
except PackageNotFoundError:
    __version__ = "0.0.0.dev0"

__all__ = ["__version__"]
