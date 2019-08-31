import os
import inspect
import json
import logging

import pandas as pd
from fuzzywuzzy import fuzz, process

import version
from settings import DATABASE_URI, OPERATOR_REGISTRY_BACKEND, OPERATORPATH
from stringprocessor import StringProcessor as sp
from deco import chained, classproperty

source = OPERATOR_REGISTRY_BACKEND
_source_is_file = False
_source_is_sql = False

logger = logging.getLogger(__name__)

if source == "sql":
    try:
        from models import Operator

        _source_is_sql = True
    except:
        logger.warning(
            f"Failed to load Operator table. Using {os.path.basename(OPERATORPATH)} for operator aliasing instead."
        )
        Operator_Table = None
        _source_is_file = True

else:
    Operator_Table = None
    _source_is_file = True

logger.info(f"Operator index backend is {source}")


__release__ = version.__release__

FUZZY_TARGET_LENGTH_MIN = 3
DSN = f"{__release__}".lower()
METHOD_TEMPLATE = __release__ + "/{function}"


# TODO: Extend stringprocessor directly?
class Operator(object):
    """Processor class tuned to identifing alises of a single operator"""

    classifier = None
    _minlen = 3
    _maxlen = 35
    _fuzzlen = 5
    _min_score = 90
    _sourced_from = DSN or "Unknown"
    _orig = None
    _name = None
    _norm = None
    alias = None

    normalize = sp.normalize

    def __init__(self, orig_name: str, classifier=None):
        try:
            self.classifier = classifier
            self.name = orig_name
            self._chain = []
            self.pscore = 0  # present score
            self.fscore = 0  # future score
            self.fuzzies = pd.DataFrame()
            self._method = None
            self.alias = self.find()

        except Exception as e:
            logger.warning(f"Unable to create Operator({self.norm}) -- {e}")
            raise

    def __str__(self):
        return f"{self.alias}"

    def __repr__(self):
        return f'<Operator: {self.norm[:self._maxlen]+"..." if self.is_long else self.norm} | {self.alias}>'  # [{self.pscore:.0f}|{self.fscore:.0f}]

    def __len__(self):
        return len(self.norm)

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, value):
        if isinstance(value, str):
            if self._orig is None:
                self._orig = value
            self._name = value
            self._norm = self.normalize(value)
            self._method = None
            self.pscore = 0
            self.fscore = 0
            self.fuzzies = pd.DataFrame()
        else:
            logger.debug(f"{value} is not an instance of str")

    @property
    def orig(self):
        return self._orig

    @property
    def tokens(self):
        return self.norm.split(" ")

    @property
    def norm(self):
        return self._norm

    @property
    def has_alias(self):
        return self.alias is not None

    @property
    def is_short(self):
        return len(self) < self._minlen

    @property
    def is_long(self):
        return len(self) > self._maxlen

    @property
    def sourced_from(self):
        return self.sourced_from or DSN

    @property
    def method(self):
        return self._method

    @method.setter
    def method(self, value):
        if self._method is None:
            self._method = self._format_method(method_name=value)
        # else:
        #     logger.debug('Classification method cannot be overwritten once set.')

    @property
    def has_classifier(self):
        return self.classifier is not None

    @property
    def json(self):
        return json.dumps(
            {
                "original": self.orig,
                "normalized": self.norm,
                "method": self.method,
                "alias": self.alias,
            }
        )

    @property
    def default_scorer(self):
        if self.classifier is not None:
            return self.classifier.default_scorer()
        else:
            return fuzz.token_set_ratio

    def _from_tokens(self, set_alias=False):
        name = ""
        tokens = self.tokens
        while len(name) < self._minlen and len(tokens) > 0:
            name += tokens.pop(0)

        # self.name = name

        return name

    def extend_lower_bound(self, pct: float = 0.1):
        self._min_score = self._min_score * (1 - pct)
        return self

    def _format_method(self, method_name: str = None):
        if not method_name:
            method_name = inspect.stack()[1][3]
        return METHOD_TEMPLATE.format(function=method_name)

    def _capture_match(self, result: pd.Series):

        try:
            if hasattr(result, "alias"):
                self.alias = result.alias
            if hasattr(result, "pscore"):
                self.pscore = result.pscore
            if hasattr(result, "fscore"):
                self.fscore = result.fscore
            if hasattr(result, "method"):
                self.method = result.method
        except Exception as e:
            logger.debug(f"operator._capture_match: {e}")
        return self

    def _exists(self, alias: str) -> bool:
        if self.classifier is not None:
            return self.classifier.exists(alias)
        else:
            return False

    @chained
    def lookup(self, name: str = None, record: pd.Series = None) -> str:

        try:
            if record is not None and not record.empty:
                result = self.classifier.lookup(record.opname)
                if not record.empty:
                    record["alias"] = result.alias
                    record["confidence"] = result.confidence
            else:
                record = self.classifier.lookup(name or self.norm)
            # else:
            #     record = self.classifier.lookup(self.norm)

            if not record.empty:
                if record.method is None:
                    record["method"] = "operator.classifier.lookup"

            # record = record.rename({'confidence': 'pscore'})
            return record
        except Exception as e:
            logger.debug(f"operator.lookup: {e}")

        return pd.Series()

    def fuzzy(self, scorer=None, score_cutoff=90, limit=1) -> pd.DataFrame:
        """Attempt to fuzzy match the target string to an operator name using the given scorer function.
        The alias for the match with the highest score is returned. If a match with a score above the cutoff
        is not found, None is returned

        Arguments:
            target {str} -- [description]

        Keyword Arguments:
            extract {str} -- 'one' or 'many'
            scorer {func} -- fuzzywuzzy scorer function
                      -- alternative scorers:
                            fuzz.token_sort_ratio -> match with tokens in an ordered set
                            fuzz.token_set_ratio -> match with tokens as a set
                            fuzz.partial_ratio -> ratio of string partials
        """

        scorer = scorer or self.default_scorer

        # result = pd.Series(name = target)
        extracted: list = process.extractBests(
            self.norm,
            self.classifier.index,
            scorer=scorer,
            limit=limit,
            score_cutoff=score_cutoff,
        )

        extracted = pd.DataFrame.from_records(extracted, columns=["opname", "fscore"])

        self.fuzzies = self.fuzzies.append(extracted)

        if limit == 1:
            return extracted.squeeze()
        else:
            return self.fuzzies

    @chained
    def ratio(self) -> pd.Series:
        result: pd.Series = self.fuzzy(
            scorer=fuzz.ratio, score_cutoff=self._min_score, limit=1
        )
        # result['method'] = 'operator.classifier.ratio'
        return result

    @chained
    def token_set(self) -> pd.Series:
        result: pd.Series = self.fuzzy(
            scorer=fuzz.token_set_ratio, score_cutoff=self._min_score, limit=1
        )
        # result['method'] = 'operator.classifier.token_set'
        return result

    @chained
    def token_sort(self) -> pd.Series:
        result: pd.Series = self.fuzzy(
            scorer=fuzz.token_sort_ratio, score_cutoff=self._min_score, limit=1
        )
        # result['method'] = 'operator.classifier.token_sort'
        return result

    def fuzzy_waterfall(self):
        result: pd.Series = self.ratio()

        if not self.is_long:
            if result.empty:
                result = self.token_set()

            if result.empty:
                result = self.token_sort()

            return self.lookup(record=result)
        else:
            return result

    @chained
    def get_alias(self) -> str:
        """Wraps internal finder method. Returns a string
           representation of the alias to the caller.

        Returns:
            str -- operator's alias
        """
        return self.find()

    @chained
    def _find(self) -> pd.Series:

        result: pd.Series = self.lookup()

        if result.empty:
            result = self.alookup()

        if result.empty:
            result = self.fuzzy_waterfall()

        if result.empty:
            result = self.seek_token()

        if not result.empty:
            self._capture_match(result)
            if self.fscore > self.pscore:
                result["name"] = self.name

        return result

    @chained
    def find(self) -> str:

        alias = None
        try:
            if self.classifier is not None:
                alias = self._find()  # .alias
                if "alias" in alias.index:
                    alias = alias.alias

            if alias is None:
                alias = self._from_tokens()

            if alias is None:
                logger.warning(
                    f"Unable to find alias for {self.name}. Defaulting to operator's original name."
                )
                alias = self.name
        except Exception as e:
            logger.warning(
                f"Error searching for alias for {self.name}. Defaulting to operator's original name. -- {e}"
            )
            alias = self.name

        logger.debug(f"Setting alias: {self.name} -> {alias}")
        return alias

    @chained
    def seek_token(self):
        result = pd.Series()
        self.name = self._from_tokens()
        result = self.lookup()
        result = self.alookup()

        return result

    @chained
    def alookup(self):
        """Look for a match in the aliases list.
        """
        result = pd.Series()
        if self._exists(self.name):
            result["alias"] = self.name
            result["fscore"] = 75
            result["pscore"] = 0
            result["method"] = "operator.classifier.alookup"

        return result

