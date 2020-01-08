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
    ):
        self.normalize = normalize
        self.aliases = aliases or {}
        self.exclude = exclude or []
        self.errors: List[str] = []
        self.parser = parser or self.parser

    def __repr__(self):
        return (
            "Transformer: %s aliases, %s exclusions",
            len(self.aliases),
            len(self.exclude),
        )

    def transform(self, row: dict) -> Row:

        try:
            row = self.drop_exclusions(row)
            row = self.apply_aliases(row)
            row = self.parser.parse(row)

            if "api14" in row.keys():
                row["api14"] = str(row["api14"])
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
        return {self.aliases[k]: v for k, v in row.items()}

    def drop_exclusions(self, row: Row) -> Row:
        if len(self.exclude) > 0:
            try:
                logger.debug(f"Dropping {len(self.exclude)} columns: {self.exclude}")
                row = {k: v for k, v in row if k not in self.exclude}
            except Exception as e:
                msg = f"Failed attempting to drop columns -- {e}"
                self.errors.append(msg)
                logger.debug(msg)
        return row


if __name__ == "__main__":

    from collector import Endpoint

    ep = Endpoint.load_from_config(conf)["registry"]
    t = Transformer(ep.mappings.aliases, ep.exclude)

    row = {
        "region": "PMI",
        "company": "Example",
        "well_name": "Example 1-30H",
        "well_api": "42461405550000",
        "frac_start_date": 43798.74804875,
        "frac_end_date": 43838.74804875,
        "surface_lat": "32.4150535",
        "surface_long": "-101.6295689",
        "bottomhole_lat": "",
        "bottomhole_long": "",
        "tvd": "8323",
        "target_formation": "Wolfcamp B",
        "start_in": "STARTED",
        "duration_in_days": "40",
        "reviewed_by": "",
        "risk_assessments": "",
        "comments": "",
        "last_upload_date": "2020-01-03 17:57:12.505",
    }

    t.transform(row)
