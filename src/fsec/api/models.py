import logging

from sqlalchemy.sql import func

from api.mixins import CoreMixin
from fsec import db

from config import get_active_config

conf = get_active_config()

logger = logging.getLogger(__name__)


class FracSchedule(CoreMixin, db.Model):
    # qualified_table_name = f"{conf.DATABASE_SCHEMA}.{conf.FRAC_SCHEDULE_TABLE_NAME}"
    __table__ = db.Model.metadata.tables[conf.FRAC_SCHEDULE_TABLE_NAME]
    __mapper_args__ = {
        "exclude_properties": conf.FRAC_SCHEDULE_EXCLUDE_PROPERTIES,
        "include_properties": conf.FRAC_SCHEDULE_INCLUDE_PROPERTIES,
    }

