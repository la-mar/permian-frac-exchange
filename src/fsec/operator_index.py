""" Class to interface with Operator database and facilitate in alias matching."""

import inspect
import json
import logging

import pandas as pd
from fuzzywuzzy import fuzz, process

import version
from settings import DATABASE_URI, OPERATOR_INDEX_SOURCE, OPERATORPATH
from stringprocessor import StringProcessor as sp
from util import chained, classproperty

source = OPERATOR_INDEX_SOURCE
_source_is_file = False
_source_is_sql = False

logger = logging.getLogger(__name__)

if source == "sql":
    from tables import Operator as Operator_Table

    _source_is_sql = True
else:
    Operator_Table = None
    _source_is_file = True

logger.info(f"Operator index backend is {source}")


__release__ = version.__release__

FUZZY_TARGET_LENGTH_MIN = 3
DSN = f"{__release__}".lower()
METHOD_TEMPLATE = __release__ + "/{function}"


class OperatorIndex(pd.DataFrame):
    # temporary properties
    _internal_names = pd.DataFrame._internal_names
    _internal_names_set = set(_internal_names)
    table = Operator_Table

    # normal properties (persistent)
    _metadata = [
        "_capture_method",
        "default_scorer",
        "normalize",
        "_exact_match",
        "_fuzzy_match",
        "_is_long_enough",
        "_inspect_fuzzy_result",
        "waterfall",
        "table",
        "update_database",
        "refresh",
    ]

    @property
    def _constructor(self):
        return OperatorIndex

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @classmethod
    def load(cls):
        pass

    @classmethod
    def save(cls, result: pd.Series) -> None:
        """Merge result into the database"""

        result = result.rename(
            {"name": "operator", "alias": "operator_alias", "fscore": "confidence"}
        )

        result = result.to_frame().T[
            ["operator", "operator_alias", "confidence", "method"]
        ]

        try:
            cls.table.merge_records(result)
            cls.table.persist()
            logger.debug(f"Updated {result.operator_alias} in database")
        except Exception as e:
            logger.error(f"Could not update database -- {e}")

    @classmethod
    def from_cache(cls):
        raise NotImplementedError()

    @classmethod
    def to_cache(cls):
        raise NotImplementedError()

    def _capture_method(self):
        return METHOD_TEMPLATE.format(function=inspect.stack()[1][3])

    @classmethod
    def default_scorer(cls):
        return fuzz.token_set_ratio

    def lookup(self, target: str) -> pd.Series:
        """Look for an exact match for the target string in a list of all operator names. If found,
        returns the alias for that name. Otherwise, returns None.
        """
        try:
            # self.refresh()
            result = pd.Series(name=target)
            if target is not None:
                result = self.loc[target].copy()
                if result.ndim > 1:
                    result = result.max()

                    result["method"] = self._capture_method()
            return result

        except KeyError as e:
            logger.debug(f"OperatorIndex: Name {target} not found.")
        except Exception as e:
            logger.error(
                f"Error looking up operator name ({e}) -- \n    Operator Name: {target}"
            )

        return result

    def exists(self, alias: str) -> bool:
        raise NotImplementedError()

    def _fuzzy_match(
        self, target: str, scorer=None, score_cutoff=85, limit=1
    ) -> pd.Series:
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

        scorer = scorer or self.default_scorer()

        # result = pd.Series(name = target)
        extracted: list = process.extractBests(
            target, self.operator, scorer=scorer, limit=limit, score_cutoff=score_cutoff
        )
        df = pd.DataFrame.from_records(
            extracted, columns=["name", "index_score", "fuzzy_score"]
        )
        return df

    def _inspect_fuzzy_result(self, extracted: pd.DataFrame):

        # if one result is passed
        if len(extracted) == 1:
            result = extracted
            best, score = result
            result = self.loc[best].copy()

            # if result.ndim > 1:
            #     result = result.max()

        else:  # if > 1 result is passed

            result.loc["method"] = self._capture_method()

            return result

    def refresh(self):
        """Get updated records from the database
        """
        new = self.table.records_updated_since(self.updated.max()).set_index("operator")
        new = new.rename(columns={"operator_alias": "alias", "fscore": "confidence"})

        if not new.empty:  # TODO: this is clunky. need to fix later
            self.update(new)
            for idx, values in new.iterrows():
                try:
                    self.loc[
                        idx
                    ]  # try to lookup the index. Insert record if the lookup fails.
                except KeyError:
                    self.loc[idx] = values

    def diverge(cls, alias1: str):
        """Assess the distance between the names of the underlying operators produce a mean
        distance from one another. If their mean distance surpasses a certain threshold, divide
        the operator names at the mean and rename the alias of those aliases in the group with the
        larger mean. (Alternatively, classify with sklean to find a natural break point.) Once spilt
        and renamed, cross validate the two groups. If an alias produces a higher score with the alias
        # from the other group, reclassify it with that other groups alias.

        """
        pass


class FileIndex(OperatorIndex):
    # temporary properties
    _internal_names = pd.DataFrame._internal_names
    _internal_names_set = set(_internal_names)

    table = None

    # normal properties (persistent)
    _metadata = ["_fp"]

    def __init__(self, *args, **kwargs):
        self._fp = kwargs.pop("path", None)
        super().__init__(*args, **kwargs)

    @property
    def _constructor(self):
        return FileIndex

    @classmethod
    def load(cls, path: str):
        """ Populate the index from a json file."""
        df = pd.read_json(path, convert_dates=["updated", "created"])
        df = FileIndex(data=df, path=path)
        if "operator" in df.columns:
            df = df.set_index("operator")
        return df

    def save(self) -> None:
        """Save Index to file"""
        try:
            js = json.loads(
                self.reset_index().to_json(orient="records", date_format="iso")
            )

            with open(self._fp, "w") as f:
                f.writelines(json.dumps(js, indent=4))
            logger.debug(f"Saved index to {self._fp}")
        except Exception as e:
            logger.error(f"Could not update database -- {e}")

    def _capture_method(self):
        return METHOD_TEMPLATE.format(function=inspect.stack()[1][3])

    @classmethod
    def default_scorer(cls):
        return fuzz.token_set_ratio

    def lookup(self, target: str) -> pd.Series:
        """Look for an exact match for the target string in a list of all operator names. If found,
        returns the alias for that name. Otherwise, returns None.
        """
        try:
            # self.refresh()
            result = pd.Series(name=target)
            if target is not None:
                result = self.loc[target].copy()
                if result.ndim > 1:
                    result = result.max()

                    result["method"] = self._capture_method()
            return result

        except KeyError as e:
            logger.debug(f"OperatorIndex: Name {target} not found.")
        except Exception as e:
            logger.error(
                f"Error looking up operator name ({e}) -- \n    Operator Name: {target}"
            )

        return result

    def exists(self, alias: str) -> bool:
        return any(self.alias == alias)

    def _fuzzy_match(
        self, target: str, scorer=None, score_cutoff=85, limit=1
    ) -> pd.Series:
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

        scorer = scorer or self.default_scorer()

        # result = pd.Series(name = target)
        extracted: list = process.extractBests(
            target, self.operator, scorer=scorer, limit=limit, score_cutoff=score_cutoff
        )
        df = pd.DataFrame.from_records(
            extracted, columns=["name", "index_score", "fuzzy_score"]
        )
        return df

    def _is_long_enough(self, name: str) -> bool:
        return len(name) >= FUZZY_TARGET_LENGTH_MIN

    def _inspect_fuzzy_result(self, extracted: pd.DataFrame):

        # if one result is passed
        if len(extracted) == 1:
            result = extracted
            best, score = result
            result = self.loc[best].copy()

            # if result.ndim > 1:
            #     result = result.max()

        else:  # if > 1 result is passed

            result.loc["method"] = self._capture_method()

            return result

    def refresh(self):
        """Get updated records from the database
        """
        new = self.table.records_updated_since(self.updated.max()).set_index("operator")
        new = new.rename(columns={"operator_alias": "alias", "fscore": "confidence"})

        if not new.empty:  # TODO: this is clunky. need to fix later
            self.update(new)
            for idx, values in new.iterrows():
                try:
                    self.loc[
                        idx
                    ]  # try to lookup the index. Insert record if the lookup fails.
                except KeyError:
                    self.loc[idx] = values

    def diverge(cls, alias1: str):
        """Assess the distance between the names of the underlying operators produce a mean
        distance from one another. If their mean distance surpasses a certain threshold, divide
        the operator names at the mean and rename the alias of those aliases in the group with the
        larger mean. (Alternatively, classify with sklean to find a natural break point.) Once spilt
        and renamed, cross validate the two groups. If an alias produces a higher score with the alias
        # from the other group, reclassify it with that other groups alias.

        """
        pass


class SQLIndex(OperatorIndex):
    # temporary properties
    _internal_names = pd.DataFrame._internal_names
    _internal_names_set = set(_internal_names)

    table = Operator_Table

    # normal properties (persistent)
    _metadata = [
        "_capture_method",
        "default_scorer",
        "normalize",
        "_exact_match",
        "_fuzzy_match",
        "_is_long_enough",
        "_inspect_fuzzy_result",
        "waterfall",
        "table",
        "update_database",
        "refresh",
    ]

    @property
    def _constructor(self):
        return OperatorIndex

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @classmethod
    def load(cls):
        """ Populate the index from a sql table. """
        df = Operator_Table.df
        df.operator = df.operator.apply(sp.normalize)
        df.operator_alias = df.operator_alias.apply(sp.normalize)
        df = df.rename(columns={"operator_alias": "alias"})
        return SQLIndex(data=df).set_index("operator")

    @classmethod
    def save(cls, result: pd.Series) -> None:
        """Merge result into the database"""

        result = result.rename(
            {"name": "operator", "alias": "operator_alias", "fscore": "confidence"}
        )

        result = result.to_frame().T[
            ["operator", "operator_alias", "confidence", "method"]
        ]

        try:
            cls.table.merge_records(result)
            cls.table.persist()
            logger.debug(f"Updated {result.operator_alias} in database")
        except Exception as e:
            logger.error(f"Could not update database -- {e}")

    @classmethod
    def from_cache(cls):
        df = pd.read_json(OPERATORPATH, convert_dates=["updated", "created"])
        df = OperatorIndex(data=df).set_index("operator")
        return df

    @classmethod
    def to_cache(cls):
        cls.to_json(OPERATORPATH, orient="records", date_format="iso")

    def _capture_method(self):
        return METHOD_TEMPLATE.format(function=inspect.stack()[1][3])

    @classmethod
    def default_scorer(cls):
        return fuzz.token_set_ratio

    def lookup(self, target: str) -> pd.Series:
        """Look for an exact match for the target string in a list of all operator names. If found,
        returns the alias for that name. Otherwise, returns None.
        """
        try:
            # self.refresh()
            result = pd.Series(name=target)
            if target is not None:
                result = self.loc[target].copy()
                if result.ndim > 1:
                    result = result.max()

                    result["method"] = self._capture_method()
            return result

        except KeyError as e:
            logger.debug(f"OperatorIndex: Name {target} not found.")
        except Exception as e:
            logger.error(
                f"Error looking up operator name ({e}) -- \n    Operator Name: {target}"
            )

        return result

    def exists(self, alias: str) -> bool:
        return any(self.alias == alias)

    def _fuzzy_match(
        self, target: str, scorer=None, score_cutoff=85, limit=1
    ) -> pd.Series:
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

        scorer = scorer or self.default_scorer()

        # result = pd.Series(name = target)
        extracted: list = process.extractBests(
            target, self.operator, scorer=scorer, limit=limit, score_cutoff=score_cutoff
        )
        df = pd.DataFrame.from_records(
            extracted, columns=["name", "index_score", "fuzzy_score"]
        )
        return df

    def _is_long_enough(self, name: str) -> bool:
        return len(name) >= FUZZY_TARGET_LENGTH_MIN

    def _inspect_fuzzy_result(self, extracted: pd.DataFrame):

        # if one result is passed
        if len(extracted) == 1:
            result = extracted
            best, score = result
            result = self.loc[best].copy()

            # if result.ndim > 1:
            #     result = result.max()

        else:  # if > 1 result is passed

            result.loc["method"] = self._capture_method()

            return result

    def diverge(cls, alias1: str):
        """Assess the distance between the names of the underlying operators produce a mean
        distance from one another. If their mean distance surpasses a certain threshold, divide
        the operator names at the mean and rename the alias of those aliases in the group with the
        larger mean. (Alternatively, classify with sklean to find a natural break point.) Once spilt
        and renamed, cross validate the two groups. If an alias produces a higher score with the alias
        # from the other group, reclassify it with that other groups alias.

        """
        pass


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
            logger.debug(f"{value} is not an instance of str.")

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
                alias = self._find().alias
            else:
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
