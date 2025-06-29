from __future__ import annotations

"""Top-level package for *upst* utilities.

This file re-exports the most frequently used public helpers so that
users can simply do::

    from upst import load_raw, load_clean, scrape_sync

rather than importing submodules individually.
"""

from .storage import load_raw, load_clean  # noqa: F401  (re-export)
from .scraper import scrape_sync  # noqa: F401  (re-export)
