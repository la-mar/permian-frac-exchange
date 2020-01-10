from __future__ import annotations
from typing import Any, Callable, List, Union, Dict
import functools
import logging
import re
from datetime import datetime
import dateutil.parser

from config import get_active_config

conf = get_active_config()

logger = logging.getLogger(__name__)


def safe_convert(func):
    """ Generic error handling decorator for primative type casts """

    @functools.wraps(func)
    def func_wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logger.debug(f"{func} failed: {e}")
            return None

    return func_wrapper


def locate_resource(name: str) -> Callable:
    """ Locate a resource (module, func, etc) within the module's namespace """
    resource = globals()[name]
    return resource


class Criterion:
    """ Basic component of validation logic used to compose a parsing rule """

    def __init__(self, func: Callable, name: str = None):
        self.name = name or ""
        self.func = func

    def __call__(self, value: Any):
        return bool(self.func(value))

    def __repr__(self):
        return f"Criterion: {self.name} - {self.func}"


class RegexCriterion(Criterion):
    """ Regex extraction harness for parser rule"""

    def __init__(self, regex: str, name: str = None):
        self.pattern = regex
        self.regex = re.compile(regex)
        super().__init__(func=self.regex.match, name=name)

    def __call__(self, value: Any):
        return super().__call__(str(value))

    def __repr__(self):
        return f"RegexCriterion: {self.name} - {self.pattern}"


class TypeCriterion(Criterion):
    """ Type check harness for parser rule """

    def __init__(self, dtype: type, name: str = None):
        func = lambda v: isinstance(v, dtype)  # noqa
        super().__init__(func=func, name=name)


class ValueCriterion(Criterion):
    """ Value comparison harness for parser rule """

    def __init__(self, value: Union[str, int, float, bool], name: str = None):
        func = lambda v: v == value  # noqa
        super().__init__(func=func, name=name)


class ParserRule:
    """ Validation rule used by a Parser to determine if/how to parse a value """

    def __init__(
        self,
        criteria: List[Criterion],
        name: str = None,
        allow_partial: bool = True,
        **kwargs,
    ):
        """
        Arguments:
            criteria {List[Criterion]} -- Criteria list

        Keyword Arguments:
            name {str} -- Rule name
            partial {bool} -- If True, the rule will pass if any criteria are satisfied.
            If False, the rule will pass only if all criteria are satisfied.

        """
        self.name = name or ""
        self.criteria = criteria
        self.allow_partial = allow_partial

    def __repr__(self):
        size = len(self.criteria)
        return f"ParserRule:{self.name} ({self.match_mode}) -  {size} criteria"

    def __call__(
        self, value: Any, return_partials: bool = False
    ) -> Union[bool, List[bool]]:
        """ Enables the ParserRule to be callable, such that invoking the rule with
            a passed value will return the evaluation result of the called rule.

            Example: MyIntegerParserRule("13") -> True

            return_partials: set to True to return a list of the result of each
                             criteria. If set to False (default), return the result of
                             the appropriate boolean operation:

                            if self.allow_partials = True: ->
                                return any([partial1, partial2, ...]
                            else:
                                return all([partial1, partial2, ...]
        """
        partials = [c(value) for c in self.criteria]
        if return_partials:
            return partials
        if self.allow_partial:
            return any(partials)
        else:
            return all(partials)

    @property
    def match_mode(self):
        """ Indicates if all criteria must be met to consider a parse successful """
        return "PARTIAL" if self.allow_partial else "FULL"

    @classmethod
    def from_list(cls, criteria: List[Dict], **kwargs) -> ParserRule:
        """ Initialize a rule from a list of criteria specifications.
                Example criteria spec:
                    criteria = \
                        [
                            {
                                "name": "parse_integers",
                                "type": "RegexCriterion",
                                "value": r"^[-+]?[0-9]+$",
                            },
                        ],
         """
        criteriaObjs: List[Criterion] = []
        for c in criteria:
            CriteriaType = locate_resource(c["type"])
            criteriaObjs.append(CriteriaType(c["value"], c["name"]))
        return cls(criteriaObjs, **kwargs)


class Parser:
    """ Parses text values according to a set of arbitrary rules """

    def __init__(
        self, rules: List[ParserRule], name: str = None, parse_dtypes: bool = True
    ):
        self.name = name or ""
        self.rules = rules
        self.parse_dtypes = parse_dtypes

    def __repr__(self):
        return f"Parser - {self.name}: {len(self.rules)} rules"

    @classmethod
    def init(cls, ruleset: Dict[str, List], name: str = None) -> Parser:
        """ Initialize from a configuration dict """
        rules: List[ParserRule] = []
        for ruledef in ruleset:
            rules.append(ParserRule.from_list(**ruledef))  # type: ignore
        return cls(rules, name=name)

    @staticmethod
    @safe_convert
    def try_int(s: str) -> int:
        # if str(s).replace("+", "").replace("-", "").isdigit():
        # return int(float(s))
        return int(s)

    @staticmethod
    @safe_convert
    def try_float(s: str) -> float:
        return float(s)

    @staticmethod
    @safe_convert
    def try_date(s: str) -> datetime:
        if s is not None:
            return dateutil.parser.parse(s)
        else:
            return s

    @staticmethod
    @safe_convert
    def try_bool(s: str) -> Union[bool, str]:
        value = str(s)
        if value.lower() == "true":
            return True
        elif value.lower() == "false":
            return False
        else:
            return s

    @staticmethod
    @safe_convert
    def try_empty_str_to_none(s: str) -> Union[None, str]:
        return None if s == "" else s

    def add_rule(self, rule: ParserRule):
        self.rules.append(rule)

    def run_checks(
        self, value: Any, return_partials: bool = False
    ) -> Union[bool, List[bool]]:
        """ Check if all parsing rules are satisfied """
        checks = []
        for Rule in self.rules:
            result = Rule(value)
            checks.append(result)
            if not result:
                logger.debug("Parser check failed: %s", (Rule,))
            else:
                logger.debug("Parser check passed: %s", (Rule,))

        return all(checks) if not return_partials else checks  # type: ignore

    def parse_dtype(self, value: str) -> Union[int, float, str, datetime]:
        funcs = [
            "try_int",
            "try_float",
            "try_date",
            "try_bool",
        ]

        for fname in funcs:
            func = getattr(self, fname)
            newvalue = func(value)
            logger.debug(
                f"Parsed dtype: %s -> %s (%s)",
                value or "None",
                newvalue or "None",
                type(newvalue).__name__,
            )
            if not isinstance(newvalue, str) and newvalue is not None:
                value = newvalue
                break

        value = self.try_empty_str_to_none(value)
        return value

    def parse(self, value: Any) -> Any:
        """ Attempt to parse a value if all checks are satisfied """
        if not self.run_checks(value):
            return value
        else:
            return self.parse_dtype(value) if self.parse_dtypes else value


if __name__ == "__main__":

    parser = Parser.init(
        conf.PARSER_CONFIG["parsers"]["default"]["rules"], name="default"
    )

    test_values = [
        None,
        "2019-01-01",
        "1",
        "+1",
        "-1",
        "2018",
        "",
        "0",
        "+0",
        "-0",
        "11",
        "00",
        "01",
        "+11",
        "+00",
        "+01",
        "-11",
        "-00",
        "-01",
        "1234567890",
        "+1234567890",
        "-1234567890",
        "1.1034",
        "+1.1034",
        "-1.1034",
        "0.1034",
        "+0.1034",
        "-0.1034",
        "11.1034",
        "00.1034",
        "01.1034",
        "+11.1034",
        "+00.1034",
        "+01.1034",
        "-11.1034",
        "-00.1034",
        "-01.1034",
        "1234567890.1034",
        "+1234567890.1034",
        "-1234567890.1034",
        "2019-01-01",
        "2020-06-06",
        "19-01-01",
        "2019-01",
        "qwe2019-01-01",
        "2019-01-01rte",
        "3242019-01-01",
        "31.24141",
        "101.98853",
        "+31.24141",
        "-101.98853",
        "9/11/2014 12:00:00 AM",
        "9/25/2014 5:00:00 AM",
        "true",
        "false",
        "True",
        "False",
        "42461405550000",
    ]

    logger.setLevel(20)
    results = []
    for value in test_values:
        new_value = parser.parse(value)
        new_value = new_value if new_value is not None else "-"
        value = value if value is not None else "-"
        results.append(new_value)
        print(f"{value:<20} -> {new_value} ({type(new_value).__name__})")
