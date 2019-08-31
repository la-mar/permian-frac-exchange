"""Classes to parse attributes from frac schedules with uncertain shape and formatting."""

from __future__ import annotations  # self return type annotations
from typing import Any
import inspect
import logging
import os
import re
from datetime import datetime
from functools import partial
from typing import Callable
from contextlib import contextmanager

import numpy as np
import pandas as pd
import pyproj
import yaml
from shapely.geometry import Point
from shapely.ops import transform

import util
from collections import Counter
from operator_index import Operator
from settings import *

logger = logging.getLogger(__name__)
logger.setLevel(LOGLEVEL)


class FrozenKeyException(Exception):
    def __init__(self, keyname: str):
        self.keyname = keyname

    def __str__(self):
        return f'"{self.keyname}" is frozen and cannot be overwritten'


class MessageCollection(dict):
    _default_counter = util.DefaultCounter

    def __init__(self, categories: list, name: str = None, frozen: bool = False):
        self._cats = categories
        self.name = name or self.__class__.__name__
        super().update({c: self._default_counter(lambda: 0) for c in categories})

    def __repr__(self):
        return f"{self.name} - " + " ".join(
            [f"{k}: {sum(v.values())}" for k, v in self.items()]
        )

    def __setitem__(self, key, value):
        if key in self.keys() and self.frozen:
            raise FrozenKeyException(key)
        super().__setitem__(key, value)

    def __missing__(self, key):
        value = self._default_counter(lambda: 0)
        self[key] = value
        return value

    def __iter__(self):
        for i in self.items():
            yield i

    def get(self, category: str = None):
        if not category:
            c = self
        else:
            c = self[category]
        return {
            k: dict(v) for k, v in c.items()
        }

    def add_message(self, category: str, msg: str):
        with self.category(category) as c:
            c[msg] += 1

    def add_category(self, category: str):
        self[category] = self._default_counter(lambda: 0)

    def register_indicators(self, parent: Any) -> None:
        for c in self.keys():
            parent.__setattr__(c, lambda: len(self[c]) > 0)

    @contextmanager
    def category(self, category: str):
        try:
            yield self[category]
        finally:
            pass


class Parser(object):
    """Utility class for importing and standardizing spreadsheets of.  It is intended to
    act as a base class for the creation of parsers applied to more specific problems.
    """

    def __init__(
        self,
        filename: str,
        alias_map: dict = None,
        dtypes: dict = None,
        target_colnames: list = None,
        exclude: list = None,
        ingest: dict = True,
        crefs=None,
    ):
        self.ingest = ingest
        self.alias_map = alias_map or {}
        self.unknown_names = []
        self.dtypes = dtypes or {}
        self.crefs = crefs  # column references
        self.filename = filename
        self.exclude = exclude or EXCLUDE_TOKENS
        self.original_column_names = []
        self.valid = True
        self.operator = None
        self._status_messages = MessageCollection(["ok", "warning", "error"])

        if dtypes is not None and target_colnames is None:
            self.target_colnames = dtypes.keys()
        else:
            self.target_colnames = target_colnames

        try:
            self.df = pd.read_excel(filename, header=None)
        except Exception as e:
            # TODO: Better error handling
            self.df = pd.DataFrame()
            logger.warning(f"Could not read file {filename} -- {e}")

        self.preprocess()

    def __repr__(self):
        return (
            f"({self.operator.name}) /parser/{self.operator.alias}/len={len(self.df)}"
        )

    @property
    def has_warnings(self):
        return self._status_messages['warning'] > 0

    @property
    def has_errors(self):
        return self._status_messages['error'] > 0

    @property
    def status(self):
        if self.has_errors:
            return 'error'
        elif self.has_warnings:
            return 'warning'
        else:
            return 'ok'

    @property
    def status_messages(self):
        return self._status_messages

    def add_status_message(self, category: str, msg: str):
        self._status_messages[category][msg] += 1

    def preprocess(self):
        """ should be executed before any other processing commands """
        self.adjust_headers()
        self.alias_columns()
        self._get_operator_name()

        return self

    def parse(self):
        self.standardize()
        self.normalize_geometry()
        self.reshape()

        return self

    def postprocess(self):
        # TODO: Generate sumamry
        pass

    def _get_operator_name(self):

        if "operator" in self.df.columns:
            opname = self.df.operator.astype(str).mode().iloc[0]
        else:
            opname = util.tokenize(
                self.filename, exclude=self.exclude, take_basename=True
            )[0]
        self.operator = Operator(opname)
        return self

    def summarize(self) -> pd.DataFrame:
        """Produces a summary of the input DataFrame

        Arguments:
        df {pd.DataFrame} -- a DataFrame

        Returns:
        pd.DataFrame -- DataFrame of summary statistics
        """
        df = self.df
        summary = df.describe().T
        summary["missing"] = len(df.index) - summary["count"]
        summary["median"] = df.median()
        summary["missing %"] = summary.missing / len(df.index) * 100
        return summary.T

    def drop_features(self, columns: list) -> pd.DataFrame:
        """ Iteratively remove the specified columns from the DataFrame if that column
            exists.

        Arguments:
            df {pd.DataFrame} -- a DataFrame
            columns {list} -- a list of column names to remove

        Returns:
            [pd.DataFrame] -- a copy of the input DataFrame with the specified
                            columns removed.
        """

        # Remove columns in the 'remove' list if they are present in the dataset
        self.df = self.df.drop(columns=[x for x in columns if x in self.df.columns])
        return self.df

    def _drop_na_cols(self) -> pd.DataFrame:
        self.df = self.df.loc[:, self.df.columns.notnull()]
        return self.df

    def _sample_colnames(self, names: list = None, n=3) -> list:
        """Randomly sample (without replacement) a subset of the names list, returning n selected itemsself.

        Arguments:
            names {list} -- list of potential column names

        Keyword Arguments:
            n {int} -- number of selections (default: {3})

        Returns:
            list
        """

        if names is None:
            names = self.target_colnames

        return np.random.choice(names, size=n, replace=False)

    def _is_valid_state(self) -> bool:
        """ Check parser state, returning True if corrupt and False if valid."""

        # TODO: function and attribute name imply the opposite of what they do
        return self.valid

    def _find_column_names(self, df=None, sensitivity=2, specificity=2) -> list:
        """Iterate rows of dataframe until a row resembling a header row is found.

        Arguments:
            df {pd.DataFrame} -- Generic dataframe

        Returns:
            tuple -- (row_index_of_header, colnames)
        """

        df = df if df is not None else self.df

        for idx, row in df.iterrows():
            cols = []
            specificity_index = pd.Series()
            for c in row:
                if isinstance(c, str):
                    c = c.lower()
                    # remove any character that is not alpha-numberic
                    c = "".join(e for e in c if e.isalnum())
                cols.append(c)
            for i in range(0, sensitivity):
                # Find the number of matches between the elements of the current row and the
                # target column names, then capture in series
                specificity_index = specificity_index.append(
                    pd.Series(len([col for col in cols if col in self.target_colnames]))
                )
            if specificity_index.mean() >= specificity:
                # Return header row index plus 1 and cleaned header names
                return idx + 1, cols
        return 0, df.columns

    def adjust_headers(self) -> Parser:
        """Look for a row that is likely to be column names and apply them to the
           columns of the DataFrame in the parser.

        Returns:selec
            Parser
        """

        df = self.df

        if self._is_valid_state():
            try:
                skiprows, df.columns = self._find_column_names(df)

                # Drop rows occuring at or before the row containing header names
                self.df = df.iloc[skiprows:]
                self.original_column_names = df.columns.tolist()

                # self.api_handler()

            except Exception as e:
                logger.error(MSG_PARSER_ERROR.format(p=self, e=e.args))
                logger.debug(e)
                self.valid = False

        return self

    def standardize(self, with_geom=True) -> Parser:
        """Apply standardization functions to the features in the dataset.

            Standardization methods applied (in order):

                - normalize api numbers to api14 (api_n)
                - identify coordinate resource system
                - cast fracstartdate to datetime
                - cast fracenddate to datetime
                - add column for operator
                - add column for operator_alias
                - validate lat/lon
                - add SHL geometry



        Keyword Arguments:
            df {pd.DataFrame} -- DataFrame of frac schedules (default: {None})

        Returns:
            self
        """

        if self._is_valid_state():

            df = self.df
            cols = df.columns.tolist()

            try:
                # self.safe_apply("api14", self.api_n)
                # self.safe_apply("api14", partial(self.api_n, n=10), apply_to="api10")
                df["crs"] = self.identify_crs(self.original_column_names)
                self.safe_apply("fracstartdate", self.date_handler)
                self.safe_apply("fracenddate", self.date_handler)

                [
                    self.safe_apply(col, self.validate_latlon)
                    for col in cols
                    if col in ["shllat", "shllon", "bhllat", "bhllon"]
                ]

                df["operator"] = self.operator.name
                df["operator_alias"] = str(self.operator.alias).upper()

                if with_geom:
                    df["geometry"] = df.apply(
                        lambda x: self.to_geometry(x.shllon, x.shllat), axis=1
                    )

                self.df = df

            except Exception as e:
                logger.exception(MSG_PARSER_ERROR.format(p=self, e=e.args), exc_info=e)
                logger.debug(e)

                self.valid = False

        return self

    def safe_apply(
        self, apply_to: str, func: Callable, apply_from: str = None
    ) -> Parser:
        try:
            logger.debug(
                f"applying {func.__name__}{f' from {apply_from}' if apply_from is not None else ''} to {apply_to}"
            )
            apply_from = apply_from or apply_to
            self.df[apply_to] = self.df[apply_from].apply(func)
        except KeyError as ke:
            logger.debug(
                MSG_PARSER_CHECK.format(p=self.operator.name, col_name=apply_from)
            )
        except Exception as e:
            logger.debug(MSG_PARSER_ERROR.format(p=self, e=e.args), exc_info=e)
        return self

    def validate_latlon(self, value):
        return self._vlatlon(value)

    def _vlatlon(self, value: str) -> float:
        """ Parses directional qualifiers (e.g. S, W) from lat/lon value, if present, then
        ensures the output lat/long has the correct sign (+/-).  This only works for North America."""

        if isinstance(value, str):
            tokens = value.split(" ")
            floats = []
            strings = []
            multi = 1
            f = 0
            for t in tokens:
                try:
                    floats.append(float(t))
                except:
                    strings.append(t.lower())
            if len(strings) > 0:
                if "w" in strings or "s" in strings:
                    multi = -1

            if len(floats) > 0:
                f = floats[0] * multi

        else:
            f = value

        # lat
        if abs(f) < 50 and f < 0:
            f = f * -1

        # lon
        elif abs(f) >= 50 and f > 0:
            f = f * -1

        return f

    def date_handler(self, value: str):
        try:
            return pd.to_datetime(value, infer_datetime_format=True)
        except:
            msg = f"Unable to convert value to datetime: {value}"
            logger.debug(msg)
            self.add_status_message('warning',msg)
            return pd.NaT

    def identify_crs(self, colnames: list = None) -> str:
        """Attempted to identify the coordinate reference system based on column
        name annotations (e.g. suffix = _nad27).

        Arguments:
            colnames {list} -- list of column names

        Returns:
            str -- crs reference tag (wgs84 or nad27)
        """

        # TODO: Parameterize annotations to list in settings

        if colnames is None:
            colnames = self.original_column_names

        if any([str(x).endswith("nad27") for x in list(colnames)]):
            return "nad27"
        else:
            return "wgs84"

    def normalize_geometry(self) -> Parser:
        """Standardize the coordinate system of the geometries in the DataFrame.

        Returns:
            Parser
        """

        df = self.df

        if self._is_valid_state():

            try:

                if "nad27" in df.crs.unique():

                    # Fix shls
                    df.geometry = df.apply(
                        lambda x: self.nad27_to_wgs84(x.geometry, x.crs), axis=1
                    )
                    df.shllon = df.geometry.apply(lambda _: _.x)
                    df.shllat = df.geometry.apply(lambda _: _.y)

                    # Fix bhls
                    bhls = df.apply(
                        lambda x: self.to_geometry(x.bhllon, x.bhllat), axis=1
                    )
                    # 'nad27'))
                    bhls = bhls.apply(lambda x: self.nad27_to_wgs84(x, x.crs))
                    df.bhllon = bhls.apply(lambda _: _.x)
                    df.bhllat = bhls.apply(lambda _: _.y)
                    df.crs = "wgs84"
                self.df = df

            except Exception as e:
                logger.error(MSG_PARSER_ERROR.format(p=self, e=e.args))
                self.valid = False

        return self

    def _verify_aliases(self, alias_map: dict = None) -> dict:
        """ If column name with a missing alias or contains an entire alias, map the alias to that name.

                Ex.
                    Before:
                          Column Name        |       Alias
                    ----------------------   |   --------------
                    scheduledfracstartdate   |       null

                    fracstartdate is in scheduledfracstartdate

                    After:

                        Column Name          |       Alias
                    ----------------------   |   --------------
                    scheduledfracstartdate   |   fracstartdate

            """

        if self._is_valid_state():

            try:

                if alias_map is None:
                    alias_map = self.alias_map

                cols = self.df.columns.tolist()

                # Capture values not in alias_map
                no_alias = []
                for i in cols:
                    if i not in alias_map.keys() and i not in alias_map.values():
                        no_alias.append(i)

                # Set alias
                for cname in no_alias:
                    for alias in list(alias_map.values()):
                        if alias in str(cname):
                            alias_map[cname] = alias
                        else:
                            self.unknown_names.append(cname)

                self.alias_map = alias_map

            except Exception as e:
                logger.error(MSG_PARSER_ERROR.format(p=self, e=e.args))
                self.valid = False

        return alias_map

    def alias_columns(self, alias_map: dict = None) -> Parser:
        """Rename columns matching a provided alias list.

        Arguments:
            alias_map {dict} -- mapping of original column names to a new name.

        Returns:
            pd.DataFrame -- DataFrame with aliased column names
        """

        if self._is_valid_state():

            df = self.df

            try:

                if alias_map is None:
                    alias_map = self.alias_map

                alias_map = self._verify_aliases(alias_map)
                self.original_column_names = df.columns.tolist()
                self.df = self.df.rename(columns=alias_map)

                # FIXME: iterate through alias_map.items() -> only apply rename if column does not already exist.
                # If column already exists, use column with the most entries. Will need seperate function.

            except Exception as e:
                logger.error(MSG_PARSER_ERROR.format(p=self, e=e.args))
                self.valid = False
                # raise

        return self

    def infer_dates(self, dstr: str) -> NotImplemented:
        # TODO: Implement better date parser
        raise NotImplementedError()

    def reshape(self, target_colnames: list = None) -> Parser:
        """Insert null columns for missing features and drop columns not in the target list,
           ensuring the final dataset conforms to the expected shape.

        Keyword Arguments:
            target_colnames {list} -- list of expected column names. If none, defaults
                                      to that of the calling parser.

        Returns:
            pd.DataFrame -- DataFrame of shape (n x len(target_colnames))
        """

        self._drop_na_cols()

        if self._is_valid_state():

            df = self.df

            try:
                if target_colnames is None:
                    target_colnames = self.target_colnames

                # Add missing columns
                for cname in target_colnames:
                    if cname not in df.columns:
                        df[cname] = None

                if df.columns.has_duplicates:
                    df = self.dedup_columns(df)

                # Drop extra columns
                df = df.drop(
                    columns=[x for x in df.columns if x not in target_colnames]
                )

                df = self.set_dtypes(df)

                self.df = df.reindex(
                    sorted([str(x) for x in df.columns.tolist()]), axis=1
                )

            except Exception as e:
                logger.error(MSG_PARSER_ERROR.format(p=self, e=e.args))
                self.valid = False
                raise

        return self

    def dedup_columns(self, df):
        """Merge columns with identical names."""

        dup_columns = df[self.df.columns[self.df.columns.duplicated()]]
        non_dup_columns = df[
            [x for x in self.df.columns if x not in dup_columns.columns.unique()]
        ]

        deduped_columns = None

        for unique_column_name in dup_columns.columns.unique():
            cols = []
            for name, col in dup_columns.items():
                cols.append(col)

            first = cols.pop()
            first = first
            for col in cols:
                first = first.astype(self.dtypes[first.name]).combine(
                    col.astype(self.dtypes[first.name]),
                    lambda x1, x2: x1 if x1 < x2 else x2,
                )

            if deduped_columns is None:
                deduped_columns = first.to_frame()
            else:
                deduped_columns = deduped_columns.join(first)

        return non_dup_columns.join(deduped_columns)

    def set_dtypes(self, df, dtypes: dict = None, errors="ignore") -> pd.DataFrame:
        """Return the input DataFrame with columns coerced to fit the types
        specified in dtypes.

        Arguments:
            df {pd.DataFrame} -- pandas DataFrame
            dtypes {dict} -- data type map where keys are column names and
            values are the desired data type.

        Returns:
            pd.DataFrame -- DataFrame with conforming data types
        """

        if self._is_valid_state():

            try:
                if dtypes is None:
                    dtypes = self.dtypes

                for c in df.columns:
                    dtype = dtypes[c]
                    if dtype == datetime:
                        df[c] = pd.to_datetime(df[c])
                    else:
                        df[c] = df[c].astype(dtype)

                # df = df.astype(dtypes, errors=errors)

            except Exception as e:
                logger.error(MSG_PARSER_ERROR.format(p=self, e=e.args))
                self.valid = False

        return df

    def tokenize_n(
        self,
        s: str,
        element: int = 0,
        exclude: list = None,
        sep: str = "_",
        basename=False,
    ) -> str:
        """Split filename into components based on the provided seperator,
        returning the nth element.

        Arguments:
            s {str} -- delimited string
            element {int} -- index of token to return from tokenized string
            exclude {list} --list of string elements to be excluded from search
            sep {str} -- delimiter used to split the input string
            basename {bool} -- call os.path.basename on the input string? Useful when s is a filepath.

        Returns:
            str -- returns nth element of tokenized string (s). Returns empty string
            if no tokens are found.
        """

        try:
            if exclude is None:
                exclude = self.exclude

            if basename:
                s = os.path.basename(s)

            # Split words in s
            words = re.findall(
                r"[\w]+", " ".join(os.path.basename(s).lower().split(sep))
            )
            words = [word for word in words if word not in exclude]
            logger.debug(f"{self.filename} tokens: {words}")
            if isinstance(words, list):
                return words[element]

        except Exception as e:
            logger.warning(
                MSG_PARSER_PARSING.format(op_name=self.operator.name, e=e, v=s)
            )

        return ""

    def api_handler(self) -> Parser:
        df = self.df
        has_api14 = "api14" in df.columns
        has_api10 = "api10" in df.columns

        if has_api14:
            self.safe_apply("api14", self.api_n)
            self.safe_apply("api10", partial(self.api_n, n=10), apply_from="api14")
        elif has_api10:
            self.safe_apply("api14", self.api_n, apply_from="api10")
        else:
            logger.error(f"{self.operator.alias}: no api number found in frac schedule")

        self.df = df
        return self

    def api_n(self, api: str, n: int = 14) -> str:
        """Standardize an api number to n number of digitsself.

        Arguments:
            api {str} -- American Petroleum Institute number

        Keyword Arguments:
            n {int} -- number of digits to which the API should be cast (default: {14})

        Returns:
            str -- transformed API number
        """

        if pd.isna(api):
            return None

        try:
            # Remove all non-digits
            api = "".join(re.findall(r"[\w]+", str(api)))
            ndigits = len(api)

            if ndigits > n:
                return api[:n]
            elif ndigits < n:
                return api + "0" * (n - ndigits)
            else:
                return api
        except Exception as e:
            logger.warning(
                MSG_PARSER_PARSING.format(op_name=self.operator.name, e=e, v=api)
            )

    def to_latlon(self, dms: str) -> NotImplemented:
        """Convert DMS to Lat Lon

        Arguments:
            dms {str} -- string containing a location expressed as
             degrees, minutes, seconds
        """
        # [SHL LAT1]+(([SHL LAT2]/60)+([SHL LAT3]/3600))
        raise NotImplementedError()

    def nad27_to_wgs84(self, geom, crs):

        try:
            if crs == "nad27":

                project = partial(
                    pyproj.transform,
                    pyproj.Proj(init="epsg:4267"),  # source coordinate system
                    pyproj.Proj(init="epsg:4326"),
                )  # destination coordinate system

                return transform(project, geom)
            else:
                return geom

        except Exception as e:
            logger.warning(
                MSG_PARSER_PARSING.format(op_name=self.operator.name, e=e, v=geom)
            )
            return None

    def to_geometry(self, lon: str, lat: str) -> Point:

        try:
            if lon is not None and lat is not None:
                if pd.isna(lon) is False and pd.isna(lat) is False:
                    return Point((float(lon), float(lat)))
            else:
                return None
        except Exception as e:
            logger.warning(
                MSG_PARSER_PARSING.format(op_name=self.operator.name, e=e, v=(lon, lat))
            )


class Parser_Factory:
    def __init__(self, **kwargs):
        self.kwargs = kwargs

    def Parser(self, filename: str):
        return Parser(filename, **self.kwargs)


class ParserCollection(pd.Series):
    """Collectively performs operations on all parsers in the collection"""

    @property
    def _constructor(self):
        return ParserCollection

    @property
    def df(self) -> pd.DataFrame:
        return pd.concat(self.apply(lambda x: x.df).tolist(), sort=False)

    def get_status(self):
        logger.debug(
            MSG_PARSER_COLLECTION_STATUS.format(
                func_name=inspect.stack()[1][3], pc_name=self.name
            )
        )

    def to_csv(self, filepath: str, **kwargs):
        self.df.to_csv(filepath, **kwargs)

    def _flatten_list(self, l: list):
        return [item for row in l for item in row]

    def normalize_geometry(self):
        self.get_status()
        logger.info("normalizing geometries...")
        return ParserCollection(self.apply(lambda x: x.normalize_geometry()))

    def adjust_headers(self):
        self.get_status()
        logger.info("adjusting headers...")
        return ParserCollection(self.apply(lambda x: x.adjust_headers()))

    def alias_columns(self):
        self.get_status()
        logger.info("aliasing column names...")
        return ParserCollection(self.apply(lambda x: x.alias_columns()))

    def reshape(self):
        self.get_status()
        logger.info("reshaping dataset...")
        return ParserCollection(self.apply(lambda x: x.reshape()))

    def collect_maps(self):
        self.get_status()
        logger.info("collecting parse maps...")
        return ParserCollection(self.apply(lambda x: x.alias_map).tolist())

    def columns(self):
        return self.apply(lambda x: x.df.columns.tolist())

    def print_columns(self):
        for x in self:
            print(x.df.columns.tolist())

    def print_shapes(self):
        for x in self:
            print(x.df.shape)

    def summarize_column_names(self):
        df = pd.DataFrame.from_records(self.columns().tolist())
        df.columns = df.iloc[0]
        return df.info()

    def ucols(self):
        headers = self.apply(lambda x: x.df.columns.tolist())
        return pd.Series(self._flatten_list(headers)).unique().tolist()

    def to_yaml(self, aliases: dict = None):
        self.get_status()
        self._unknowns_to_yaml(aliases)
        self._aliases_to_yaml(aliases)

    def _unknowns_to_yaml(self, aliases: dict):
        """Save previously unseen operator names to a YAML file.

        Arguments:
            aliases
        """

        # Save unknown names to yaml
        unique_columns = self.ucols()

        # load previously unseen names
        if os.path.isfile(UNKNOWNPATH):
            with open(UNKNOWNPATH, "r") as f:
                no_alias = yaml.safe_load(f) or []

        try:

            for i in unique_columns:
                if i not in no_alias + list(aliases.keys()) + list(aliases.values()):
                    no_alias.append(i)
            no_alias = [x for x in no_alias if x not in ["nan", ".nan", np.nan]]

            if os.path.isfile(UNKNOWNPATH):
                with open(UNKNOWNPATH, "w") as f:
                    yaml.safe_dump(no_alias, f)

        except Exception as e:
            logger.error(f"Unable to write unknown column names to alias file. -- {e}")

    def _aliases_to_yaml(self, aliases: dict):

        try:
            for x in self.collect_maps():
                aliases.update(x)

            if os.path.isfile(ALIASPATH):
                with open(ALIASPATH, "w") as f:
                    yaml.safe_dump(aliases, f)

        except Exception as e:
            logger.error(f"Unable to write new column aliases to alias file. -- {e}")

    def log_parsers(self):
        self.get_status()
        logger.debug([parser for parser in self])


if __name__ == "__main__":

    if os.path.isfile(ALIASPATH):
        with open(ALIASPATH, "r") as f:
            alias_map = yaml.safe_load(f)

    factory = Parser_Factory(
        alias_map=alias_map, dtypes=COLUMN_DTYPES, target_colnames=COLUMN_NAMES
    )

    parsers = {}
    path = None
    if path is None:
        path = os.path.abspath(FSEC_DOWNLOAD_DIR)

    if os.path.isdir(path):
        paths = os.listdir(path)

    else:  # is file
        paths = [path]
        path = os.path.basename(path)
    for f in paths:
        f = f"{path}/{f}"
        if os.path.isfile(f):
            p = factory.Parser(f)
            parsers[p.operator.alias] = p

    p = parsers["crownquest"]
    self = p

    ps = ParserCollection(parsers, name="FracSchedules")
    ps = ps.adjust_headers()
    ps = ps.alias_columns()
    ps = ps.standardize()
    ps = ps.normalize_geometry()
    ps = ps.reshape()
