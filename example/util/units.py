# Constant for converting Nautical Miles to meters
from typing import Final


NM_TO_METERS: Final[float] = 1852.0
METERS_TO_NM: Final[float] = 1.0 / NM_TO_METERS
MPS_TO_KNOTS: Final[float] = 1.94384449
KNOTS_TO_MPS: Final[float] = 1.0 / MPS_TO_KNOTS

def format_timedelta_human(td):
    """Formats a timedelta object into a more human-readable string."""
    total_seconds = td.total_seconds()
    if total_seconds < 60:
        return f"{total_seconds:.3f} seconds"

    minutes, seconds = divmod(total_seconds, 60)
    if minutes < 60:
        return f"{int(minutes)}m {int(seconds)}s"

    hours, minutes = divmod(minutes, 60)
    return f"{int(hours)}h {int(minutes)}m {int(seconds)}s"