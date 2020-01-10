# pylint: disable=arguments-differ, method-hidden

from typing import Any
import json
from datetime import datetime, date, timedelta


class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        return super(DateTimeEncoder, self).default(obj)


class ObjectEncoder(json.JSONEncoder):
    """Class to convert an object into JSON."""

    def default(self, obj: Any):
        """Convert `obj` to JSON."""
        if hasattr(obj, "to_json"):
            return self.default(obj.to_json())
        elif hasattr(obj, "__dict__"):
            return obj.__class__.__name__
        elif hasattr(obj, "tb_frame"):
            return "traceback"
        elif isinstance(obj, timedelta):
            return obj.__str__()
        else:
            # generic, captures all python classes irrespective.
            cls = type(obj)
            result = {
                "__custom__": True,
                "__module__": cls.__module__,
                "__name__": cls.__name__,
            }
            return result
