from __future__ import annotations
from typing import List, Dict

import logging
from config import get_active_config

import util

from collector.parser import Parser

conf = get_active_config()

sp = util.StringProcessor()

logger = logging.getLogger(__name__)


class RowParser(object):
    """ Transform an XML response into a normalized Python object"""

    def __init__(
        self,
        aliases: Dict[str, str] = None,
        exclude: List[str] = None,
        normalize: bool = False,
        parsers: List[Parser] = None,
    ):
        self.normalize = normalize
        self.aliases = aliases or {}
        self.exclude = exclude or []
        self.parsers = parsers or []
        self.errors: List[str] = []

    def __repr__(self):
        s = "s" if len(self.parsers) > 1 else ""
        return f"RowParser: {len(self.parsers)} attached parser{s}"

    def add_parser(
        self, parser: Parser = None, ruleset: Dict[str, List] = None, name: str = None
    ):
        self.parsers.append(parser or Parser.init(ruleset, name=name))
        return self

    def normalize_keys(self, data: Dict) -> Dict:
        return util.apply_transformation(data, sp.normalize, keys=True, values=False)

    def parse_value_dtypes(self, data: Dict) -> Dict:
        for parser in self.parsers:
            data = util.apply_transformation(
                data, parser.parse, keys=False, values=True
            )
        return data

    def parse(self, row: dict, parse_dtypes: bool = True, **kwargs) -> Dict:
        # parsed = self.normalize_keys(row)
        if parse_dtypes:
            parsed = self.parse_value_dtypes(row)
        return parsed

    @staticmethod
    def load_from_config(parser_conf: dict):
        parser_conf = parser_conf.get("parsers", parser_conf)
        parsers: List[Parser] = []
        for name, parser_def in parser_conf.items():
            ruleset = parser_def.get("rules", parser_def)
            parsers.append(Parser.init(ruleset, name))

        return RowParser(parsers=parsers)


if __name__ == "__main__":
    rp = RowParser.load_from_config(conf.PARSER_CONFIG)

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
    rp.parse(row)
