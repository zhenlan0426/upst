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
import html
import re

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


def _clean_content(content: Any) -> str | None:
    """Clean HTML content by decoding entities and stripping tags.
    
    Converts HTML-encoded job posting content into clean, readable plain text.
    This includes:
    1. Decoding HTML entities (&lt; → <, &gt; → >, &amp; → &, etc.)
    2. Stripping all HTML tags
    3. Normalizing whitespace (multiple spaces/newlines → single space)
    4. Handling special cases like &nbsp; and other whitespace entities
    """
    
    if content is None:
        return None
    
    content_str = str(content).strip()
    if not content_str:
        return None
    
    # Decode HTML entities (e.g., &lt; → <, &gt; → >, &amp; → &, &nbsp; → space)
    decoded = html.unescape(content_str)
    
    # Remove HTML tags using regex
    # This regex matches opening tags, closing tags, and self-closing tags
    clean_text = re.sub(r'<[^>]+>', '', decoded)
    
    # Handle special whitespace cases:
    # 1. Replace multiple whitespace chars (spaces, tabs, newlines) with single space
    # 2. Handle any remaining HTML entities that might have been missed
    clean_text = re.sub(r'\s+', ' ', clean_text)
    
    # Remove any remaining HTML artifacts or special characters
    # Handle cases like &nbsp; that might still be present
    clean_text = re.sub(r'&[a-zA-Z0-9#]+;', ' ', clean_text)
    
    # Final whitespace cleanup - replace multiple spaces with single space
    clean_text = re.sub(r' +', ' ', clean_text)
    
    # Strip leading/trailing whitespace
    clean_text = clean_text.strip()
    
    return clean_text if clean_text else None


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
    5. ``content``             → cleaned HTML content (entities decoded, tags stripped)

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
        "content": _clean_content,
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