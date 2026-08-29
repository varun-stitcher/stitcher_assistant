"""Chargeback period resolution — ported from SPC ``tools/chargeback.py`` ``_resolve_period``.

Three modes, in priority order:

1. ``period="YYYY-MM"`` — full calendar month. e.g. ``"2026-03"`` → 2026-03-01 .. 2026-04-01
   (exclusive end), label ``"March 2026"``.
2. ``period="last_month"`` — the most recent full calendar month (always month-aligned).
3. Fallback — rolling ``since_days`` window ending today.
"""

from __future__ import annotations

import calendar
import re
from datetime import date, timedelta

# YYYY-MM with month in 01..12 (stricter than ``\\d{1,2}`` — catches 2026-99 / 2026-13 at the
# regex layer before they fall through to ``date()`` and produce a less helpful error).
_PERIOD_RE = re.compile(r"^(\d{4})-(0[1-9]|1[0-2])$")


def resolve_period(period: str | None, since_days: int) -> tuple[date, date, str]:
    """Resolve a chargeback time window to ``(start, end_exclusive, label)``."""
    if period:
        token = period.strip().lower()
        if token == "last_month":
            today = date.today()
            this_month_start = today.replace(day=1)
            end = this_month_start  # exclusive
            start = (this_month_start - timedelta(days=1)).replace(day=1)
            label = f"{calendar.month_name[start.month]} {start.year}"
            return start, end, label

        m = _PERIOD_RE.match(period.strip())
        if not m:
            raise ValueError(
                f"period must be 'YYYY-MM' (with month 01..12) or 'last_month', got {period!r}. "
                "For natural language month names, convert first (e.g. 'March 2026' → '2026-03')."
            )
        year, month = int(m.group(1)), int(m.group(2))
        start = date(year, month, 1)
        end = date(year + 1, 1, 1) if month == 12 else date(year, month + 1, 1)
        label = f"{calendar.month_name[month]} {year}"
        return start, end, label

    end = date.today() + timedelta(days=1)  # inclusive of today's data
    start = end - timedelta(days=since_days)
    label = f"{start.isoformat()} to {(end - timedelta(days=1)).isoformat()}"
    return start, end, label


__all__ = ["resolve_period"]
