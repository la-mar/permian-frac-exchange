from datetime import datetime
import subprocess

import pytest  # noqa

from api.models import FracSchedule
from sqlalchemy.exc import IntegrityError


@pytest.fixture
def records():
    yield [
        {
            "api14": "00000000000000",
            "frac_start_date": datetime.now(),
            "frac_end_date": datetime.now(),
            "shllat": 31,
            "shllon": -101,
        },
        {
            "api14": "00000000000001",
            "frac_start_date": datetime.now(),
            "frac_end_date": datetime.now(),
            "shllat": 31,
            "shllon": -101,
        },
    ]


class TestModelMixins:
    def test_bulk_insert(self, app, records):

        subprocess.run(["fracx", "db", "recreate"])
        with app.app_context():
            FracSchedule.bulk_insert(records)

    def test_core_insert_ignore_on_conflict(self, app, records):

        subprocess.run(["fracx", "db", "recreate"])
        with app.app_context():
            FracSchedule.core_insert(
                records, update_on_conflict=False, ignore_on_conflict=True
            )

    def test_core_insert_update_on_conflict(self, app, records):

        subprocess.run(["fracx", "db", "recreate"])
        with app.app_context():
            FracSchedule.core_insert(
                records, update_on_conflict=False, ignore_on_conflict=True
            )

    def test_get_pks(self, app, records):

        subprocess.run(["fracx", "db", "recreate"])
        with app.app_context():
            FracSchedule.core_insert(
                records, update_on_conflict=False, ignore_on_conflict=True
            )

            pks = [x[0] for x in FracSchedule.pks]
            expected = [x["api14"] for x in records]
            assert pks == expected

    def test_bulk_merge(self, app, records):

        subprocess.run(["fracx", "db", "recreate"])
        with app.app_context():
            FracSchedule.bulk_merge(records)

            pks = [x[0] for x in FracSchedule.pks]
            expected = [x["api14"] for x in records]
            assert pks == expected

    def test_persist_objects(self, app, records):

        subprocess.run(["fracx", "db", "recreate"])
        objs = [FracSchedule(**o) for o in records]
        with app.app_context():
            FracSchedule.persist_objects(objs)

            pks = [x[0] for x in FracSchedule.pks]
            expected = [x["api14"] for x in records]
            assert pks == expected

    def test_core_insert_invalid_data(self, app):
        records = [
            {
                "api14": "00000000000002",
                "frac_start_date": datetime.now(),
                "frac_end_date": datetime.now(),
                "shllat": 31,
                "shllon": -101,
            },
            {
                "api14": "00000000000001",
                "frac_start_date": datetime.now(),
                "frac_end_date": datetime.now(),
                "shllat": 31,
                "shllon": -101,
            },
            {
                "api14": "00000000000000",
                "frac_start_date": datetime.now(),
                "frac_end_date": datetime.now(),
                "shllat": 31,
                "shllon": -101,
            },
        ]

        subprocess.run(["fracx", "db", "recreate"])
        with app.app_context():
            FracSchedule.core_insert(records)
            with pytest.raises(IntegrityError):
                FracSchedule.bulk_insert(records)

    def test_get_primary_key_names(self, app):
        with app.app_context():
            assert FracSchedule.primary_key_names() == [
                "api14",
                "frac_start_date",
                "frac_end_date",
            ]

