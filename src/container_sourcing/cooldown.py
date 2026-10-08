"""Local source cooldown timestamps; this module does not schedule any work."""
from datetime import timedelta
from zoneinfo import ZoneInfo
import os

from .models import utcnow


def next_due(now=None):
    now = (now or utcnow()).astimezone(ZoneInfo('Asia/Kolkata'))
    hour, minute = map(int, os.environ.get('SOURCING_DAILY_TIME', '06:00').split(':'))
    scheduled = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
    return scheduled if scheduled > now else scheduled + timedelta(days=1)
