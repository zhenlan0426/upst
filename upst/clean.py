"""Data-cleaning utilities transforming Upstart job-posting data into a 
flat, analysis-ready format.

The scraper can optionally apply these cleaning transformations directly 
during the scraping process. When enabled (default), the scraper stores 
cleaned data to data/clean/ instead of raw nested JSON to data/raw/.

Many columns in the raw Greenhouse API response contain nested structures – 
lists of dictionaries or plain dictionaries – which are awkward to work with 
in downstream analytics.

This module provides functions to *normalise* the nested columns so that every 
cell is a simple scalar (string / int / bool / None).

The transformation rules are pragmatic rather than exhaustive – we retain only
the most useful fields for typical investor analysis. Raw data can still be 
obtained by calling the scraper with clean_data=False if deeper inspection 
is required.
"""

from typing import Any, List
import json
import os

import pandas as pd

__all__ = [
    "clean_nested_columns",
]


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------

def _safe_json_loads(val: Any) -> Any:
    """Attempt to ``json.loads`` *val* if it looks like a JSON-encoded string.

    Otherwise return *val* unchanged.  This is handy because nested structures
    round-trip through Parquet as *strings* but sometimes they are already real
    ``dict``/``list`` objects (e.g. when operating on in-memory scraper output).
    """

    if isinstance(val, str):
        val = val.strip()
        if (val.startswith("[") and val.endswith("]")) or (
            val.startswith("{") and val.endswith("}")
        ):
            try:
                return json.loads(val)
            except (json.JSONDecodeError, TypeError):
                # Fall through – treat as opaque string.
                pass
    return val


def _flatten_location(loc: Any) -> str | None:
    """Normalise the ``location`` field to a plain human-readable string."""

    loc = _safe_json_loads(loc)
    if isinstance(loc, dict):
        return str(loc.get("name") or loc)
    return str(loc) if loc is not None else None


def _list_of_dicts_to_names(val: Any, *, key: str = "name") -> str | None:
    """Convert ``[{key: ...}, …]`` or variant into a comma-separated string."""
    import numpy as np
    
    val = _safe_json_loads(val)
    
    # Handle numpy arrays by converting to list
    if isinstance(val, np.ndarray):
        val = val.tolist()
    
    if isinstance(val, list):
        names: List[str] = []
        for item in val:
            if isinstance(item, dict):
                if key in item and item[key]:
                    names.append(str(item[key]))
            else:
                names.append(str(item))
        return ", ".join(names) if names else None
    return str(val) if val is not None else None


def _serialise_opaque(val: Any) -> str | None:
    """Serialise complex objects to a JSON string for archival purposes."""

    val = _safe_json_loads(val)
    if isinstance(val, (dict, list)):
        try:
            return json.dumps(val, separators=(",", ":"), ensure_ascii=False)
        except (TypeError, ValueError):
            pass
    return str(val) if val is not None else None


# ---------------------------------------------------------------------------
# Public cleaning routine
# ---------------------------------------------------------------------------

def clean_nested_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Return *df* with nested columns flattened / serialised.

    The operation is *pure* – it returns a **new** DataFrame and does not mutate
    the input argument.

    Rules applied column-by-column:

    1. ``location``            → extracted ``name`` (string)
    2. ``departments``         → comma-separated list of department names
    3. ``offices``             → comma-separated list of office locations
    4. ``data_compliance``     → JSON string (kept for completeness)

    Any additional columns containing nested data can be appended to
    ``_NESTED_COLUMNS`` below with an appropriate handler.
    """

    if df.empty:
        # Nothing to do.
        return df.copy()

    # Create a shallow copy so we do not mutate the caller's frame.
    out = df.copy(deep=False)

    # Mapping: column_name → transformation function.
    _NESTED_COLUMNS = {
        "location": _flatten_location,
        "departments": _list_of_dicts_to_names,
        "offices": _list_of_dicts_to_names,
        "data_compliance": _serialise_opaque,
        "company_name": lambda x: None,  # Remove this redundant field
    }

    for col, func in _NESTED_COLUMNS.items():
        if col in out.columns:
            if func is None or (callable(func) and func.__name__ == '<lambda>' and func(None) is None):
                # Drop the column entirely
                out = out.drop(columns=[col])
            else:
                out[col] = out[col].map(func)

    return out 