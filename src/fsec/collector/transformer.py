from __future__ import annotations
from typing import Dict, List, Union
import logging
from datetime import date, datetime


from collector.parser import Parser
from collector.row_parser import RowParser
from config import get_active_config


conf = get_active_config()

logger = logging.getLogger(__name__)

Scalar = Union[int, float, str, None, datetime, date]
Row = Dict[str, Scalar]


class TransformationError(Exception):
    pass


class Transformer(object):
    parser = RowParser.load_from_config(conf.PARSER_CONFIG)

    def __init__(
        self,
        aliases: Dict[str, str] = None,
        exclude: List[str] = None,
        normalize: bool = False,
        parser: Parser = None,
        ignore_unknown: bool = True,
    ):
        self.normalize = normalize
        self.aliases = aliases or {}
        self.exclude = exclude or []
        self.errors: List[str] = []
        self.parser = parser or self.parser
        self.ignore_unknown = ignore_unknown

    def __repr__(self):
        la = len(self.aliases)
        le = len(self.exclude)
        unknown = "permissive" if self.ignore_unknown else "strict"
        return f"Transformer: {la} aliases, {le} exclusions ({unknown})"

    def transform(self, row: dict) -> Row:

        try:
            row = self.drop_exclusions(row)
            row = self.apply_aliases(row)
            row = self.parser.parse(row)

            if "api14" in row.keys():
                api14 = str(row["api14"])
                ndiff = 14 - len(api14)
                if ndiff > 0:
                    api14 += "0" * ndiff
                row["api14"] = api14
                row["api10"] = row["api14"][:10]

            numerrs = len(self.errors)
            if len(self.errors) > 0:
                logger.warning(
                    "Captured %s parsing errors during transformation: %s",
                    numerrs,
                    self.errors,
                )

            return row
        except Exception as e:
            logger.exception(f"Transformation error: {e}")
            raise TransformationError(e)

    def apply_aliases(self, row: Row) -> Row:
        return {self.aliases[k]: v for k, v in row.items() if k in self.aliases.keys()}

    def drop_exclusions(self, row: Row) -> Row:
        keys = list(row.keys())
        exclude = [*self.exclude]
        if self.ignore_unknown:
            exclude += [k for k in keys if k not in self.aliases.keys()]
        if len(exclude) > 0:
            logger.debug(f"Dropping {len(exclude)} columns: {exclude}")
            try:
                row = {k: v for k, v in row.items() if k not in exclude}
            except Exception as e:
                msg = f"Failed attempting to drop columns -- {e}"
                self.errors.append(msg)
                logger.debug(msg)
        return row


if __name__ == "__main__":

    from collector import Endpoint

    logging.basicConfig(level=10)
    logger.setLevel(10)

    ep = Endpoint.load_from_config(conf)["frac_schedules"]
    t = Transformer(ep.mappings.aliases, ep.exclude)

    row = {
        "region": "PMI",
        "company": "Example",
        "well_name": "Example 1-30H",
        "well_api": "4246140555",
        "frac_start_date": 43798.74804875,
        "frac_end_date": 43838.74804875,
        "surface_lat": "32.4150535",
        "surface_long": "-101.6295689",
        "bottomhole_lat": "32.4150535",
        "bottomhole_long": "-101.6295689",
        "tvd": "8323",
        "target_formation": "Wolfcamp B",
        "start_in": "STARTED",
        "duration_in_days": "40",
        "reviewed_by": "",
        "risk_assessments": "",
        "comments": "",
        "last_upload_date": "2020-01-03 17:57:12.505",
    }

    parsed = t.transform(row)

    parsed
    from datetime import datetime

    # datetime.fromordinal(parsed["frac_start_date"])

    t.errors
