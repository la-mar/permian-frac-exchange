from datetime import datetime

import pytest  # noqa

from collector.parser import (
    Parser,
    Criterion,
    RegexCriterion,
    ValueCriterion,
    TypeCriterion,
    ParserRule,
)

# TODO: incorporate hypothesis


@pytest.fixture
def parser(conf):
    yield Parser.init(conf.PARSER_CONFIG["parsers"]["default"]["rules"], name="default")


@pytest.fixture
def arbitrary_callable():
    def return1(value):
        return 1

    yield return1


@pytest.fixture
def rule():
    fc = RegexCriterion(r"^[-+]?\d*\.\d+$")
    ic = RegexCriterion(r"^[-+]?[0-9]+$")
    yield ParserRule(criteria=[fc, ic], allow_partial=True)


class TestParser:
    def test_parse_signed_int(self, parser):
        inputs = [
            "+00001",
            "+01",
            "+1",
            "+11",
            "-00001",
            "-01",
            "-1",
            "-11",
            "+0",
            "-0",
            "+00",
            "-00",
            "+00000",
            "-00000",
        ]
        expected = [1, 1, 1, 11, -1, -1, -1, -11, 0, 0, 0, 0, 0, 0]

        result = [parser.parse(x) for x in inputs]
        assert result == expected

    def test_parse_unsigned_int(self, parser):
        inputs = ["1", "10"]
        expected = [1, 10]

        result = [parser.parse(x) for x in inputs]
        assert result == expected

    def test_parse_signed_float(self, parser):
        inputs = [
            "+1.1034",
            "-1.1034",
            "+0.1034",
            "-0.1034",
            "+11.1034",
            "+00.1034",
            "+01.1034",
            "-11.1034",
            "-00.1034",
            "-01.1034",
            "+1234567890.1034",
            "-1234567890.1034",
            "+31.24141",
            "-101.98853",
        ]
        expected = [
            1.1034,
            -1.1034,
            0.1034,
            -0.1034,
            11.1034,
            0.1034,
            1.1034,
            -11.1034,
            -0.1034,
            -1.1034,
            1234567890.1034,
            -1234567890.1034,
            31.24141,
            -101.98853,
        ]

        result = [parser.parse(x) for x in inputs]
        assert result == expected

    def test_parse_unsigned_float(self, parser):
        inputs = [
            "1.1034",
            "0.1034",
            "11.1034",
            "00.1034",
            "01.1034",
            "1234567890.1034",
            "31.24141",
            "101.98853",
        ]

        expected = [
            1.1034,
            0.1034,
            11.1034,
            0.1034,
            1.1034,
            1234567890.1034,
            31.24141,
            101.98853,
        ]

        result = [parser.parse(x) for x in inputs]
        assert result == expected

    def test_parse_datetime(self, parser):
        inputs = [
            "2019-01-01",
            "2019/01/01",
            "19-01-01",
            "9/11/2014 12:00:00 AM",
            "9/25/2014 5:00:00 AM",
        ]
        expected = [
            datetime(year=2019, month=1, day=1),
            datetime(year=2019, month=1, day=1),
            datetime(year=2001, month=1, day=19),
            datetime(year=2014, month=9, day=11, hour=0),
            datetime(year=2014, month=9, day=25, hour=5),
        ]

        result = [parser.parse(x) for x in inputs]
        assert result == expected

    def test_ignore_clouded_datetime(self, parser):
        inputs = [
            "qwe2019-01-01",
            "2019-01-01rte",
            "3242019-01-01",
        ]
        assert parser.parse_many(inputs) == inputs

    def test_ignore_incomplete_datetime(self, parser):
        inputs = ["2019-01"]
        assert parser.parse_many(inputs) == inputs

    def test_parse_bool(self, parser):
        inputs = ["true", "True", "false", "False"]
        expected = [True, True, False, False]

        result = [parser.parse(x) for x in inputs]
        assert result == expected

    def test_parser_repr(self, parser):
        repr(parser)

    def test_parser_rule_repr(self, rule):
        repr(rule)

    def test_try_date(self, parser):
        assert parser.try_date("2019-01-01") == datetime(2019, 1, 1)

    def test_try_date_handle_none(self, parser):
        assert parser.try_date(None) is None

    def test_add_rule(self, rule):
        parser = Parser(rules=[rule, rule])
        parser.add_rule(rule)
        assert len(parser.rules) == 3


class TestCriterion:
    def test_criterion_repr_works(self):
        repr(Criterion(lambda x: 1, name="test"))

    def test_regexcriterion_repr_works(self):
        repr(RegexCriterion(r"\w", name="test"))

    def test_criterion_callable(self):
        c = Criterion(lambda x: 1, name="test")
        assert c(1) == 1

    def test_regex_criterion(self):
        rc = RegexCriterion("\w")
        assert rc("test value") is True

    def test_type_criterion_int(self):
        tc = TypeCriterion(int)
        assert tc(1) is True

    def test_type_criterion_string_is_not_int(self):
        tc = TypeCriterion(str)
        assert tc(1) is False

    def test_value_criterion_parse_value(self):
        vc = ValueCriterion(123)
        assert vc(123) is True

    def test_rule_repr(self, rule):
        repr(rule)

    def test_rule_return_partials(self, rule):
        assert rule("123.321", return_partials=True) == [True, False]

    def test_get_match_mode(self, rule):
        assert rule.match_mode == "PARTIAL"

    def test_toggle_match_mode(self, rule):
        rule.allow_partial = False
        assert rule.match_mode == "FULL"

    def test_partial_parse(self, rule):
        assert rule("123") is True
        assert rule("132.32") is True
        assert rule("test553.23") is False
        assert rule("55test") is False
        assert rule("55.123test") is False
        assert rule("test55.123") is False

    def test_full_parse(self):
        fc = RegexCriterion(r"^[-+]?\d*\.\d+$")
        fc2 = RegexCriterion(r"^[-+]?\d*\.\d+$")
        rule = ParserRule(criteria=[fc, fc2], allow_partial=False)
        assert rule("132.32") is True
        assert rule("test553.23") is False
        assert rule("55test") is False
        assert rule("55.123test") is False
        assert rule("test55.123") is False

