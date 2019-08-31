from abc import ABC, abstractmethod
from datetime import datetime
import pytz


class BaseHandler(ABC):
    @abstractmethod
    def check(self):
        pass


class DateHandler(BaseHandler):
    tz = pytz.timezone("US/Central")

    def safe_localize(self, dt: datetime) -> datetime:
        try:
            if dt:  # bypasses None, NaN, and NaT
                if not isinstance(dt, pd.Timestamp):
                    dt = pd.to_datetime(dt)
                dt = dt.tz_localize(self.tz)
        except:
            logger.debug(f"Value not localized timestamp -> {dt}")

        return dt


class NaNHandler(BaseHandler):
    def nan_to_none(d: dict):
        for k, v in d.items():
            if v in ["NaN", "NaT", "None"]:
                d[k] = None
        return d

