"""Utility functions."""
import os


def removefile_unchecked(filepath: str):
    """Remove file and ignore errors."""
    try:
        os.remove(filepath)
    except:  # pylint: disable=W0702
        pass
