# pylint: disable=no-self-use
import pytest

from util.strings import StringProcessor

SPECIAL_CHARS = "!@#$%^&*()"


@pytest.fixture
def sp():
    yield StringProcessor()


class TestStringProcessor:
    def test_sp_props(self, sp):
        assert sp.replacement == "_"
        assert sp.tolower is True
        assert sp.toupper is False

    def test_remove_special_chars(self, sp):
        s = sp.alphanum_only(f"test123{SPECIAL_CHARS}")
        s = sp.fill_whitespace(s, "")
        assert s == "test123"

    def test_dedupe_whitespace(self, sp):
        assert sp.dedupe_whitespace("test  123") == "test 123"

    def test_remove_all_whitespace(self, sp):
        assert sp.remove_whitespace(" test 123 ") == "test123"

    def test_normalize_string(self, sp):
        assert sp.normalize(f"test 123 {SPECIAL_CHARS}") == "test_123"

    def test_normalize_string_to_int(self, sp):
        i = int(sp.normalize(f"123 {SPECIAL_CHARS}", int_compatable=True))
        assert i == 123

    def test_replace_all_non_numeric(self, sp):
        assert int(sp.numeric_only("test123")) == 123

    def test_normalize_to_uppercase(self):
        sp = StringProcessor(toupper=True)
        assert sp.normalize("test 123") == "TEST_123"
