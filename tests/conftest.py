from __future__ import annotations

"""Pytest configuration shared across all test modules.

We *explicitly* add the project root directory (one level up from the *tests*
package) to ``sys.path`` so that ``import upst`` works even when the working
directory inside the test runner is not the project root.

This approach keeps the repository lightweight – we do not need to create a
full setuptools/pyproject installation just to make tests importable.
"""

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT)) 