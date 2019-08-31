""" Class to interface with Operator registry backend and facilitate in alias matching."""

from __future__ import annotations
from typing import List, Set, Dict, Tuple, Optional, Callable, Any, Union
import inspect
import json
import logging
import os
from abc import ABC, abstractmethod
from datetime import datetime

import pandas as pd
from fuzzywuzzy import fuzz, process

import models
import version
from stringprocessor import StringProcessor as sp
from deco import chained, classproperty, indicate
from util.types import Scalar
from yammler import Yammler

logger = logging.getLogger(__name__)

__release__ = version.__release__

FUZZY_TARGET_LENGTH_MIN = 3
DSN = f"{__release__}".lower()
METHOD_TEMPLATE = __release__ + "/{function}"


def get_release():
    v: str = "unknown"

    if "version" in globals():
        try:
            v = version.__release__
        except AttributeError as ae:
            logger.debug(f"version.py has no attribute '__release__' -- {ae}")

    return v


class RegistryBase(ABC):
    def __init__(self, *args, **kwargs):
        super().__init__()

    @abstractmethod
    def load(cls) -> None:
        pass

    @abstractmethod
    def save(self) -> None:
        """Persist result to the backend"""
        pass

    @abstractmethod
    def refresh(self) -> None:
        """Reload data from the backend"""
        pass

    @abstractmethod
    def lookup(self, key) -> str:
        pass

    @abstractmethod
    def add(self, key, value) -> None:
        pass

    @abstractmethod
    def remove(self, key) -> None:
        pass

    @abstractmethod
    def closest(self, key) -> str:
        pass

    @abstractmethod
    def handle_date(self, value):
        pass


class RegistryBackend(RegistryBase):
    _value_key = "value"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._cls = kwargs.pop("cls", dict)  # optional class for return type
        self._value_key = kwargs.pop("value_key", self._value_key)

    @property
    def value_key(self):
        return self._value_key

    # TODO: @indicate
    def load(cls) -> None:
        pass

    def save(self) -> None:
        """Persist result to the backend"""
        pass

    def refresh(self) -> None:
        """Reload data from the backend"""
        pass

    def lookup(self, key) -> str:
        pass

    def add(self, key, value) -> None:
        pass

    def remove(self, key) -> None:
        pass

    def closest(self, key) -> str:
        pass

    def handle_date(self, value):
        pass

    @staticmethod
    def stamp():
        return datetime.utcnow()


class DataFrameBackend(RegistryBackend, pd.DataFrame):

    # temporary properties
    _internal_names = pd.DataFrame._internal_names
    _internal_names_set = set(_internal_names)

    # normal properties (persistent)
    _metadata = ["load", "save", "refresh"]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @property
    def _constructor(self):
        return self.__class__

    @classmethod
    def load(cls):
        pass

    def save(self) -> None:
        """Persist result to the backend"""
        pass

    def refresh(self) -> None:
        """Persist result to the backend"""
        pass


class FileBackend(RegistryBackend):
    def __init__(self, fspath: str, interface=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._fspath: str = fspath
        self._interface = interface  # Yammler

    @property
    def fspath(self):
        return self._fspath

    def load(
        self, index_col: str = "operator", date_cols: list = ["updated", "created"]
    ) -> pd.DataFrame:
        """ Populate the index from a json file."""
        df = pd.read_json(self.fspath, convert_dates=date_cols)
        try:
            df = df.set_index(index_col)
        except KeyError:
            raise KeyError(
                f"Backend has no column named '{index_col}'. Try passing 'index_col = column_name' to the backend constructor. Available columns are: {df.columns.tolist()}"
            )
        self.source = df
        return self

    def save(self) -> None:
        """Persist the index"""
        try:
            js = json.loads(
                self.reset_index().to_json(orient="records", date_format="iso")
            )

            with open(self._fspath, "w") as f:
                f.writelines(json.dumps(js, indent=4))
            logger.debug(f"Persisted registry to {self._fspath}")
        except Exception as e:
            logger.error(f"Failed to save {self.__class__} -- {e}")

    def refresh(self) -> None:
        pass

    def lookup(self) -> None:
        pass

    def add(self, key) -> str:
        pass

    def remove(self, key) -> str:
        pass

    def closest(self, key) -> str:
        pass

    def _link(self):
        """ Retrieve link to persistance mechanism """
        return self._interface(self.fspath)


class YamlBackend(RegistryBackend):
    """Interface for a YAML backed registry. Data is manipulated in an in-memory
    Pandas' DataFrame. Changes are persisted on command. The interface for this backend differs from others in that the step to persist changes is explicit. A customized interface to the YAML file on disk can be substituted using the 'interface' keyword.  """

    _df = None
    _yaml = None

    def __init__(
        self,
        fspath: str,
        date_cols: list = None,
        interface: Any = Yammler,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.date_cols = date_cols or []
        self._fspath = fspath

    @property
    def fspath(self):
        return self._fspath

    @property
    def df(self):
        return self._df

    @property
    def yaml(self):
        if self._yaml is None:
            self._yaml = self._link()
        return self._yaml

    # TODO: Generalize and move to parent
    @indicate
    def load(self) -> YamlBackend:
        """ Populate the index """
        self._df = pd.DataFrame(self.yaml).T

        return self

    @property
    def defaults(self):
        return self._defaults

    @defaults.setter
    def defaults(self, key, value) -> None:
        self._defaults[key] = value

    def save(self) -> None:
        """Persist the index"""
        try:
            self.yaml.overwrite(self.df.to_dict(orient="index")).dump(force=True)
            logger.debug(f"Persisted registry to {self.fspath}")
        except Exception as e:
            logger.error(f"Failed to save {self.__class__} -- {e}")

    def refresh(self) -> None:
        # TODO: save records updated since load time and update self.data with new records available in the backend
        return self.load()

    def lookup(self, key) -> self._cls:
        try:
            return self._cls(**self.df.loc[key].to_dict())
        except KeyError:
            logger.debug(f"No entry found for '{key}'")
            return self._cls()

    def add(self, key, value: Union[Scalar, dict]) -> str:
        existing = dict(self.lookup(key))
        new = dict.fromkeys(self.df.columns.tolist())
        new.update({"created_at": self.stamp(), "updated_at": self.stamp()})
        new.update(existing)
        if isinstance(value, dict):
            new[self.value_key].update(value)
        else:
            new[self.value_key] = value

        self._df.loc[key] = new

    def remove(self, key) -> str:
        return 0

    def closest(self, key) -> str:
        return 0

    def _encode_dates(self) -> pd.DataFrame:
        df = self._df.copy(deep=True)
        if df is not None:
            for col in self.date_cols:
                try:
                    df[col] = df[col].apply(self.handle_date)
                    df[col] = df[col].astype(str)
                except Exception as e:
                    logger.debug(f"Error encoding dates in column '{col}' -- {e}")

        return df

    def handle_date(self, dt, default: Callable = None) -> datetime:
        try:
            dt = pd.to_datetime(
                dt, infer_datetime_format=True, errors="raise", utc=True
            )  # assume unknown timezones are UTC
            if not dt.tzname():
                dt = dt.localize("UTC")
            elif dt.tzname() != "UTC":
                dt = dt.convert("UTC")
            return dt
        except:
            logger.debug(f"Failed converting value to datetime -> {dt}")
            if default:
                return default()
            else:
                return pd.Timestamp.now()


class SQLBackend(RegistryBackend):
    import models

    models.connect_db()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @classmethod
    def load(cls, table_name: str, index_col: str = "operator"):
        """ Populate the index from a sql table"""
        # df = Operator_Table.df
        # df.operator = df.operator.apply(sp.normalize)
        # df.operator_alias = df.operator_alias.apply(sp.normalize)
        # df = df.rename(columns={"operator_alias": "alias"})
        try:
            import models

            cnxn = models.connect_db()
            cnxn["Base"].prepare(Base.metadata.bind)
            op = Operator
            op.cnames()
            # TODO: Connect this up

        except KeyError:
            raise KeyError(
                f"Backend has no column named '{index_col}'. Try passing 'index_col = column_name' to the backend constructor. Available columns are: {df.columns.tolist()}"
            )
        return df

    def save(self) -> None:
        """Persist the index"""
        try:
            js = json.loads(
                self.reset_index().to_json(orient="records", date_format="iso")
            )

            with open(self._fspath, "w") as f:
                f.writelines(json.dumps(js, indent=4))
            logger.debug(f"Persisted registry to {self._fspath}")
        except Exception as e:
            logger.error(f"Failed to save {self.__class__} -- {e}")


class Registry:
    def __init__(self, backend: RegistryBackend):
        self._backend = backend

    @property
    def backend(self):
        return self._backend

    def load(self, *args, **kwargs) -> self:
        self.backend.load(*args, **kwargs)
        return self

    def save(self, *args, **kwargs) -> self:
        self.backend.save(*args, **kwargs)
        return self

    @classmethod
    def from_cache(cls):
        pass

    @classmethod
    def to_cache(cls):
        pass

    def _capture_method(self):
        caller = inspect.stack()[1][3]
        return METHOD_TEMPLATE.format(function=caller)

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
            logger.debug(f"Registry: Name {target} not found.")
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

    def add(op_name: str, op_alias: str):
        pass

    def remove(op_name: str, op_alias: str):
        pass


class OperatorRegistry(Registry):
    pass


class FileIndex(Registry):
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
            logger.debug(f"Registry: Name {target} not found.")
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


class SQLIndex(Registry):
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
        return Registry

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
        df = Registry(data=df).set_index("operator")
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
            logger.debug(f"Registry: Name {target} not found.")
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


if __name__ == "__main__":

    from settings import OPERATORPATH

    logging.basicConfig(level=logging.DEBUG)

    OPERATOR_YAML = "./config/operators.yaml"
    y = Yammler(OPERATOR_YAML)
    yb = YamlBackend(OPERATOR_YAML, value_key="alias").load()

    r = Registry(backend=yb)
    opr = OperatorRegistry()

    # x = Operator("DRIFTWOOD ENERGY", classifier=sqlindex)
    x = Operator("lario3mofracsched8", classifier=fileindex)
    print(x)

    o = Operator("lario3mofracsched8", classifier=fileindex)

    fi.df = fi.df.rename(
        columns={
            "sourced_from": "source",
            "created": "created_at",
            "updated": "updated_at",
            "method": "methodology",
        }
    )

