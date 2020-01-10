import logging

from sqlalchemy.sql import func

from api.mixins import CoreMixin
from config import get_active_config
from fracx import db

conf = get_active_config()

logger = logging.getLogger(__name__)


class FracSchedule(CoreMixin, db.Model):

    __tablename__ = conf.FRAC_SCHEDULE_TABLE_NAME

    api14 = db.Column(db.String(14), nullable=False, primary_key=True)
    api10 = db.Column(db.String(10), nullable=False)
    wellname = db.Column(db.String(), nullable=True)
    operator = db.Column(db.String())
    frac_start_date = db.Column(db.Date(), primary_key=True)
    frac_end_date = db.Column(db.Date(), primary_key=True)
    status = db.Column(db.String())
    tvd = db.Column(db.Integer())
    target_formation = db.Column(db.String())
    shllat = db.Column(db.Float())
    shllon = db.Column(db.Float())
    bhllat = db.Column(db.Float())
    bhllon = db.Column(db.Float())
    created_at = db.Column(
        db.DateTime(timezone=True), default=func.now(), nullable=False
    )
    updated_at = db.Column(
        db.DateTime(timezone=True), default=func.now(), nullable=False
    )
