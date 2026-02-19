#!/usr/bin/env python3
"""Build traceability mart for Thai funds effective exposure."""

from __future__ import annotations

import sys
from pathlib import Path

if __package__ is None or __package__ == "":
    sys.path.append(str(Path(__file__).resolve().parents[2]))

from etl.jobs.traceability.main import main


if __name__ == "__main__":
    raise SystemExit(main())
