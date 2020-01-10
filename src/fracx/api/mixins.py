# type: ignore

from __future__ import annotations

import logging
from enum import Enum
from timeit import default_timer as timer
from typing import Dict, List

from sqlalchemy.dialects.postgresql.dml import Insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.sql import func

import metrics
import util
from util.deco import classproperty
from fracx import db


class Operation(Enum):
    INSERT = "insert"
    UPDATE = "update"
    DELETE = "delete"
    MERGE = "merge"


class TimestampMixin(object):
    created_at = db.Column(
        db.DateTime(timezone=True), default=func.now(), nullable=False
    )
    updated_at = db.Column(
        db.DateTime(timezone=True), default=func.now(), nullable=False
    )


logger = logging.getLogger(__name__)


class CoreMixin(object):
    """Base class for sqlalchemy ORM tables containing mostly utility functions for
       accessing table properties and managing insert, update, and upsert operations.
    """

    pks = None

    @classproperty
    def s(self):
        return db.session

    @classproperty
    def pks(cls) -> List[tuple]:  # noqa
        query = cls.s.query().with_entities(*cls.primary_key_columns())
        return query.all()

    @classmethod
    def primary_key_columns(cls) -> List:
        """Returns a list of sqlalchemy column objects for this table's primary keys.

        Returns:
            list -- [Column1, Column2, ...]
        """

        # return [v for k, v in cls.__table__.c.items() if v.primary_key]
        return list(cls.__table__.primary_key.columns)

    @classmethod
    def primary_key_names(cls) -> List[str]:
        """Returns the column names of this table's primary keys.

        Returns:
            list -- column names
        """

        return list(cls.__table__.primary_key.columns.keys())

    @classmethod
    def persist_objects(cls, objects: List[db.Model]):
        cls.s.add_all(objects)
        cls.persist()
        logger.info(
            f"{cls.__table__.name}.persist_objects: inserted {len(objects)} records"
        )

    @classmethod
    def persist(cls) -> None:
        """Propagate changes in session to database.
        """
        try:
            cls.s.flush()
            cls.s.commit()
        except Exception as e:
            logger.info(e)
            cls.s.rollback()

    @classmethod
    def core_insert(
        cls,
        records: List[Dict],
        size: int = None,
        exclude_cols: list = None,
        update_on_conflict: bool = True,
        ignore_on_conflict: bool = False,
    ):
        op_name = "core_insert"
        affected: int = 0
        size = size or len(records)
        exclude_cols = exclude_cols or []
        for chunk in util.chunks(records, size):
            ts = timer()
            chunk = list(chunk)
            stmt = Insert(cls).values(chunk)

            # update these columns when a conflict is encountered
            if ignore_on_conflict:
                final_stmt = stmt.on_conflict_do_nothing(
                    constraint=cls.__table__.primary_key
                )
                op_name = op_name + "_ignore_on_conflict"
            elif update_on_conflict:
                on_conflict_update_cols = [
                    c.name
                    for c in cls.__table__.c
                    if c not in list(cls.__table__.primary_key.columns)
                    and c.name not in exclude_cols
                ]
                op_name = op_name + "_update_on_conflict"

                # append 'on conflict' clause to insert statement
                final_stmt = stmt.on_conflict_do_update(
                    constraint=cls.__table__.primary_key,
                    set_={
                        k: getattr(stmt.excluded, k) for k in on_conflict_update_cols
                    },
                )

            else:
                final_stmt = stmt
            try:
                cls.s.bind.engine.execute(final_stmt)
                cls.persist()
                exc_time = round(timer() - ts, 2)
                n = len(chunk)
                cls.post_op_metrics(Operation.INSERT, op_name, n, exc_time)
                affected += len(records)

            except IntegrityError as ie:
                logger.debug(ie.args[0])

                # fragment and reprocess
                if len(records) > 1:
                    first_half = records[: len(records) // 2]
                    second_half = records[len(records) // 2 :]
                    cls.core_insert(
                        records=first_half,
                        size=len(first_half) // 4,
                        update_on_conflict=update_on_conflict,
                        ignore_on_conflict=ignore_on_conflict,
                    )
                    cls.core_insert(
                        records=second_half,
                        size=len(second_half) // 4,
                        update_on_conflict=update_on_conflict,
                        ignore_on_conflict=ignore_on_conflict,
                    )
            except Exception as e:
                logger.error(e)

        return affected

    @classmethod
    def bulk_insert(cls, records: List[Dict], size: int = None):

        affected: int = 0
        size = size or len(records)

        for chunk in util.chunks(records, size):

            cls.s.bulk_insert_mappings(cls, chunk)
            cls.persist()
            logger.info(
                f"{cls.__table__.name}.bulk_insert: inserted {len(records)} records"
            )
            affected += len(records)
        return affected

    @classmethod
    def bulk_update(cls, records: List[Dict], size: int = None):

        affected: int = 0
        size = size or len(records)

        for chunk in util.chunks(records, size):
            cls.s.bulk_update_mappings(cls, chunk)
            cls.persist()
            logger.info(
                f"{cls.__table__.name}.bulk_update: updated {len(records)} records"
            )
            affected += len(records)
        return affected

    @classmethod
    def bulk_merge(cls, records: List[Dict], size: int = None):
        affected: int = 0
        size = size or len(records)

        for chunk in util.chunks(records, size):
            ts = timer()
            chunk = list(chunk)
            cls.s.add_all([cls.s.merge(cls(**row)) for row in chunk])
            cls.persist()
            exc_time = round(timer() - ts, 2)
            n = len(chunk)
            cls.post_op_metrics(Operation.MERGE, "bulk_merge", n, exc_time)
            affected += len(records)

        return affected

    @classmethod
    def primary_key_values(cls, as_list: bool = False) -> List[tuple]:
        query = cls.s.query().with_entities(*cls.primary_key_columns())
        return query.all()

    @classmethod
    def post_op_metrics(
        cls, method_type: Operation, method: str, n: int, exc_time: float
    ):
        op_name = method_type.name.lower()
        tags = {"tablename": cls.__table__.name, "method": method}
        measurements = {
            f"{op_name}s": n,
            f"{op_name}_time": exc_time,
            f"{op_name}s_per_second": n / exc_time or 1,
        }

        for key, value in measurements.items():
            metrics.post(key, value, tags=tags)

        logger.info(
            f"{cls.__table__.name}.{method}: {op_name}ed {n} records ({exc_time}s)",
            extra=measurements,
        )
