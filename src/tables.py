"""Module encapsulting a portable wrapper for interfacing with a SQL
table via sqlalchemy's ORM to manage frac_schedules
"""


import logging
import time


from geopandas import GeoDataFrame
import pandas as pd
import shapely
from shapely.geometry.base import BaseGeometry
from sqlalchemy import MetaData, Table, create_engine, func, exc
from sqlalchemy.event import listens_for
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.sql.functions import GenericFunction
from sqlalchemy.types import UserDefinedType
import warnings


from src.util import classproperty
from src.settings import (DATABASE_URI,
                          FRAC_SCHEDULE_TABLE,
                          OPERATOR_TABLE,
                          EXCLUSIONS,
                          LOAD_GEOMETRY,
                          WGS84)

warnings.simplefilter("ignore", category=exc.SAWarning)
logger = logging.getLogger(__name__)


engine = create_engine(DATABASE_URI)
metadata = MetaData(bind=engine)
session_factory = sessionmaker(bind=engine)
Session = scoped_session(session_factory)


Base = declarative_base()


@listens_for(session_factory, "pending_to_persistent")
@listens_for(session_factory, "deleted_to_persistent")
@listens_for(session_factory, "detached_to_persistent")
@listens_for(session_factory, "loaded_as_persistent")
def detect_all_persistent(session, instance):
    print("object is now persistent: %s" % instance)


class GenericTable(object):
    """Base class for sqlalchemy ORM tables containing mostly utility
    functions for accessing table properties and managing insert, update,
    and upsert operations.
    """


    # __bind_key__ = None
    # __table_args__ = None
    # __mapper_args__ = None
    # __tablename__ = None

    session = None

    @classproperty
    def df(cls):
        query = cls.session.query(cls)
        return pd.read_sql(query.statement, query.session.bind)

    @classmethod
    def pk_names(cls) -> list:
        """Returns the column names of this table's primary keys.

        Returns:
            list -- column names
        """

        return cls.__table__.primary_key.columns.keys()

    @classmethod
    def pk_column_objects(cls) -> list:
        """Returns a list of sqlalchemy column objects for this table's primary keys.

        Returns:
            list -- [Column1, Column2, ...]
        """

        return [v for k, v in cls.__table__.c.items() if v.primary_key]

    @classmethod
    def pk_values(cls):
        return cls.session.query(cls).with_entities(*cls.pk_column_objects()).all()

    @classmethod
    def keys(cls):
        query = cls.session.query(cls).with_entities(*cls.pk_column_objects())
        return list(pd.read_sql(query.statement, query.session.bind).squeeze().values)

    @classmethod
    def keyedkeys(cls):
        # return self.df[[self.aliases['id']]].to_dict('records')
        query = cls.session.query(cls).with_entities(*cls.pk_column_objects())
        return pd.read_sql(query.statement, query.session.bind).to_dict('records')

    @classmethod
    def cnames(cls):
        return cls.__table__.columns.keys()

    @classmethod
    def ctypes(cls):
        return {colname: col.type for colname, col in
                cls.__table__.c.items()
                }

    @classmethod
    def ptypes(cls):
        return {colname: col.type.python_type for colname, col in
                cls.__table__.c.items()
                }

    @classmethod
    def get_existing_records(cls):
        return cls.session.query(cls).all()

    @classmethod
    def get_session_state(cls, count=True) -> dict:
        if cls.session is not None:
            if count:
                return {'new': len(cls.session.new),
                        'updates': len(cls.session.dirty),
                        'deletes': len(cls.session.deleted)
                        }
            else:
                return {'new': cls.session.new,
                        'updates': cls.session.dirty,
                        'deletes': cls.session.deleted
                        }

    @classmethod
    def merge_records(cls, df: pd.DataFrame, print_rec: bool = False) -> None:
        """Convert dataframe rows to object instances and merge into session by
        primary key matching.

        Arguments:
            df {pd.DataFrame} -- A dataframe of object attributes.

        Keyword Arguments:
            print {bool} -- Optional: Print record when inserting.

        Returns:
            None
        """
        # Drop rows with NA in a primary key
        df = df.dropna(subset=cls.pk_names())
        logger.info(f'Records to be inserted: {len(df)}')
        merged_objects = []
        nrecords = len(df)
        nfailed = 0
        for i, row in enumerate(df.iterrows()):
            try:

                merged_objects.append(
                    cls.session.merge(
                        cls(
                            **row[1].where(~pd.isna(row[1]), None).to_dict()
                            )
                        )
                    )
                if print_rec == True:
                    logger.info(f'{cls.__tablename__}: loaded {i} of {nrecords}')

            except Exception as e:
                logger.error(
                    f'''Failed to merge record: --''' + '\n\n' \
                    f'''Invalid record: {i-1}/{len(df)}'''+'\n' \
                    f'''    {row[1]}''' + '\n' \
                    f''' {e} '''

                )
                nfailed += 1

        # Add merged objects to session
        cls.session.add_all(merged_objects)
        logger.info(
            f'Successfully loaded {nrecords-nfailed} records to {cls.__tablename__}')

    @classmethod
    def get_last_update(cls):
        """Get the datetime of the most recently updated record

        Returns:
            datetime
        """

        return cls.session.query(func.max(cls.__table__.c.updated)).first()

    @classmethod
    def nrows(cls):
        """Return a count of the number of rows in this table.

        Returns:
            int -- row count
        """

        return cls.session.query(func.count(cls.__table__.c[cls.pk_names()[0]])).first()

    @classmethod
    def load_updates(cls, updates: list) -> None:
        """Add all objects in the input list to the table's session and
           auto-commit the changes.

        Arguments:
            updates {list} -- list of ORM objects for this table

        Returns:
            None
        """

        try:
            cls.session.add_all(updates)
            cls.session.commit()
        except Exception as e:
            cls.session.rollback()
            logger.info('Could not load updates')
            logger.info(e)

    @classmethod
    def load_inserts(cls, inserts: pd.DataFrame) -> None:

        try:
            insert_records = []
            # To dict to pass to sqlalchemy
            for row in inserts.to_dict('records'):

                # Create record object and add to dml list
                insert_records.append(cls(**row))
            cls.session.add_all(insert_records)

            # Commit Insertions
            cls.session.commit()
        except Exception as e:
            cls.session.rollback()
            logger.info('Could not load inserts')
            logger.info(e)

    @classmethod
    def persist(cls) -> None:
        """Propagate changes in session to database.

        Returns:
            None
        """
        try:
            logger.info(cls.get_session_state())
            cls.session.commit()
            logger.info(f'Persisted to {cls.__tablename__}')
        except Exception as e:
            logger.info(e)
            cls.session.rollback()


class Geometry(UserDefinedType):
    """Custom type to make SQL Server geometries compatable
       with sqlalchemy.

       NOTE: Geometry use in sqlalchemy is only supported for MSSQL.
       """

    def get_col_spec(self):
        return "GEOMETRY"

    def bind_expression(self, bindvalue):
        return func.geo.STGeomFromText(bindvalue, WGS84, type_=self)

    def column_expression(self, col):
        return func.geo.STAsText()


class STGeomFromText(GenericFunction):
    """  """
    type = Geometry
    package = "geo"
    name = "GEOMETRY::STGeomFromText"
    identifier = "STGeomFromText"


class STAsText(GenericFunction):
    type = Geometry
    package = "geo"
    name = "GEOMETRY.STAsText"
    identifier = "STAsText"


class Operator(GenericTable, Base):
    """ competitor.dbo.operator"""

    __table_args__ = {'autoload': True,
                      'autoload_with': engine,
                      'schema': 'dbo'}
    __tablename__ = OPERATOR_TABLE
    session = Session()


class frac_schedule(GenericTable, Base):
    __table_args__ = {'autoload': True,
                     'autoload_with': engine,
                    'schema': 'dbo'}
    __tablename__ = FRAC_SCHEDULE_TABLE
    __mapper_args__ = {
        # 'exclude_properties': EXCLUSIONS,
        'include_properties': ['api10',
                                'api14',
                                'bhllat',
                                'bhllon',
                                'fracenddate',
                                'fracstartdate',
                                'operator',
                                'operator_alias',
                                'shllat',
                                'shllon',
                                'tvd',
                                'wellname']
    }
    session = Session()


    def __repr__(self):
        return f'frac_schedule: {self.api14}: {self.operator_alias} | {self.operator}'


def nullloads(geom):
    if not pd.isna(geom):
        if isinstance(geom, BaseGeometry):
            return geom
        return shapely.wkt.loads(geom)


def to_wkt(geom):
    if not pd.isna(geom):
        return geom.wkt


def frame_to_db(df: str, table: Table = frac_schedule):

    if df is None:
        logger.info('No data to load in DataFrame.')
        return None

    drop = ['crs', 'nan', pd.np.nan]

    if LOAD_GEOMETRY:

        if 'geometry' in df.columns:
            g = GeoDataFrame(df, geometry=df.geometry.apply(
                nullloads), crs={'init': 'epsg:' + str(WGS84)})

            g['wkt'] = g.geometry.apply(to_wkt)

            g = g.rename(columns={'wkt': 'geometry'})
        else:
            g = df
    else:
        drop += ['geometry']
        g = df


    for colname in drop:
        try:
            g = g.drop(columns = [colname])
        except Exception as e:
            logger.debug(f'{e}')

    t0 = time.time()
    # return g
    table.merge_records(g, print_rec=False)
    table.persist()
    logger.info(f'ORM merge_records(): Total time for {str(len(g))} records ({time.time() - t0:.2f}) secs')


if __name__ == "__main__":

      fs = frac_schedule
