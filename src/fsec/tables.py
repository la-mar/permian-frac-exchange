# """Module encapsulting a portable wrapper for interfacing with a SQL
# table via sqlalchemy's ORM to manage FracSchedules
# """


# import logging
# import time
# import warnings
# from datetime import datetime

# import pandas as pd
# import shapely
# from geopandas import GeoDataFrame
# from shapely.geometry.base import BaseGeometry
# from sqlalchemy import MetaData, Table, create_engine, exc, func, Column

# # from sqlalchemy.event import listens_for
# from sqlalchemy.orm import scoped_session, sessionmaker
# from sqlalchemy.sql.functions import GenericFunction
# from sqlalchemy.types import UserDefinedType
# from sqlalchemy import Integer, String, Sequence, DateTime, Date, Float
# from sqlalchemy.engine.url import URL
# from sqlalchemy.ext.declarative import declarative_base, DeferredReflection

# from settings import (
#     EXCLUSIONS,
#     FRAC_SCHEDULE_TABLENAME,
#     LOAD_GEOMETRY,
#     OPERATOR_TABLENAME,
#     WGS84,
#     DATABASE_NAME,
#     DATABASE_SCHEMA,
#     DATABASE_URL_PARAMS,
#     CREATED_QUALIFIER,
#     UPDATED_QUALIFIER,
# )
# from util import classproperty

# warnings.simplefilter("ignore", category=exc.SAWarning)
# logger = logging.getLogger(__name__)


# def make_url(url_params: dict):
#     return URL(**url_params)


# class GenericTable(object):
#     """Base class for sqlalchemy ORM tables containing mostly utility
#     functions for accessing table properties and managing insert, update,
#     and upsert operations.
#     """

#     # __bind_key__ = None
#     # __table_args__ = None
#     # __mapper_args__ = None
#     # __tablename__ = None

#     session = None

#     @property
#     def s(self):
#         return self.session

#     @classproperty
#     def df(cls):
#         query = cls.session.query(cls)
#         return pd.read_sql(query.statement, query.session.bind)

#     @classmethod
#     def pk_names(cls) -> list:
#         """Returns the column names of this table's primary keys.

#         Returns:
#             list -- column names
#         """

#         return cls.__table__.primary_key.columns.keys()

#     @classmethod
#     def pk_column_objects(cls) -> list:
#         """Returns a list of sqlalchemy column objects for this table's primary keys.

#         Returns:
#             list -- [Column1, Column2, ...]
#         """

#         return [v for k, v in cls.__table__.c.items() if v.primary_key]

#     @classmethod
#     def pk_values(cls):
#         return cls.session.query(cls).with_entities(*cls.pk_column_objects()).all()

#     @classmethod
#     def keys(cls):
#         query = cls.session.query(cls).with_entities(*cls.pk_column_objects())
#         return list(pd.read_sql(query.statement, query.session.bind).squeeze().values)

#     @classmethod
#     def keyedkeys(cls):
#         # return self.df[[self.aliases['id']]].to_dict('records')
#         query = cls.session.query(cls).with_entities(*cls.pk_column_objects())
#         return pd.read_sql(query.statement, query.session.bind).to_dict("records")

#     @classmethod
#     def cnames(cls):
#         return cls.__table__.columns.keys()

#     @classmethod
#     def ctypes(cls):
#         return {colname: col.type for colname, col in cls.__table__.c.items()}

#     @classmethod
#     def ptypes(cls):
#         return {
#             colname: col.type.python_type for colname, col in cls.__table__.c.items()
#         }

#     @classmethod
#     def get_existing_records(cls):
#         return cls.session.query(cls).all()

#     @classmethod
#     def get_session_state(cls, count=True) -> dict:
#         if cls.session is not None:
#             if count:
#                 return {
#                     "new": len(cls.session.new),
#                     "updates": len(cls.session.dirty),
#                     "deletes": len(cls.session.deleted),
#                 }
#             else:
#                 return {
#                     "new": cls.session.new,
#                     "updates": cls.session.dirty,
#                     "deletes": cls.session.deleted,
#                 }

#     @classmethod
#     def merge_records(cls, df: pd.DataFrame, print_rec: bool = False) -> None:
#         """Convert dataframe rows to object instances and merge into session by
#         primary key matching.

#         Arguments:
#             df {pd.DataFrame} -- A dataframe of object attributes.

#         Keyword Arguments:
#             print {bool} -- Optional: Print record when inserting.

#         Returns:
#             None
#         """
#         # Drop rows with NA in a primary key
#         df = df.dropna(subset=cls.pk_names())
#         logger.info(f"Records to be inserted: {len(df)}")
#         merged_objects = []
#         nrecords = len(df)
#         nfailed = 0
#         for i, row in enumerate(df.iterrows()):
#             try:

#                 merged_objects.append(
#                     cls.session.merge(
#                         cls(**row[1].where(~pd.isna(row[1]), None).to_dict())
#                     )
#                 )
#                 if print_rec == True:
#                     logger.info(f"{cls.__tablename__}: loaded {i} of {nrecords}")

#             except Exception as e:
#                 logger.error(
#                     f"""Failed to merge record: --""" + "\n\n"
#                     f"""Invalid record: {i-1}/{len(df)}""" + "\n"
#                     f"""    {row[1]}""" + "\n"
#                     f""" {e} """
#                 )
#                 nfailed += 1

#         # Add merged objects to session
#         cls.session.add_all(merged_objects)
#         logger.info(
#             f"Successfully loaded {nrecords-nfailed} records to {cls.__tablename__}"
#         )

#     @classmethod
#     def get_last_update(cls):
#         """Get the datetime of the most recently updated record

#         Returns:
#             datetime
#         """

#         return cls.session.query(func.max(cls.__table__.c.updated)).first()

#     @classmethod
#     def nrows(cls):
#         """Return a count of the number of rows in this table.

#         Returns:
#             int -- row count
#         """

#         return cls.session.query(func.count(cls.__table__.c[cls.pk_names()[0]])).first()

#     @classmethod
#     def load_updates(cls, updates: list) -> None:
#         """Add all objects in the input list to the table's session and
#            auto-commit the changes.

#         Arguments:
#             updates {list} -- list of ORM objects for this table

#         Returns:
#             None
#         """

#         try:
#             cls.session.add_all(updates)
#             cls.session.commit()
#         except Exception as e:
#             cls.session.rollback()
#             logger.info("Could not load updates")
#             logger.info(e)

#     @classmethod
#     def load_inserts(cls, inserts: pd.DataFrame) -> None:

#         try:
#             insert_records = []
#             # To dict to pass to sqlalchemy
#             for row in inserts.to_dict("records"):

#                 # Create record object and add to dml list
#                 insert_records.append(cls(**row))
#             cls.session.add_all(insert_records)

#             # Commit Insertions
#             cls.session.commit()
#         except Exception as e:
#             cls.session.rollback()
#             logger.info("Could not load inserts")
#             logger.info(e)

#     @classmethod
#     def persist(cls) -> None:
#         """Propagate changes in session to database.

#         Returns:
#             None
#         """
#         try:
#             logger.info(cls.get_session_state())
#             cls.session.commit()
#             logger.info(f"Persisted to {cls.__tablename__}")
#         except Exception as e:
#             logger.info(e)
#             cls.session.rollback()


# class Geometry(UserDefinedType):
#     """Custom type to make SQL Server geometries compatable
#        with sqlalchemy.

#        NOTE: Geometry use in sqlalchemy is only supported for MSSQL.
#        """

#     def get_col_spec(self):
#         return "GEOMETRY"

#     def bind_expression(self, bindvalue):
#         return func.geo.STGeomFromText(bindvalue, WGS84, type_=self)

#     def column_expression(self, col):
#         return func.geo.STAsText()

#     def python_type(self):
#         return shapely.geom


# class STGeomFromText(GenericFunction):
#     """  """

#     type = Geometry
#     package = "geo"
#     name = "GEOMETRY::STGeomFromText"
#     identifier = "STGeomFromText"


# class STAsText(GenericFunction):
#     type = Geometry
#     package = "geo"
#     name = "GEOMETRY.STAsText"
#     identifier = "STAsText"


# # TODO: Refactor so it doesnt run on import
# def connect_db():
#     """ Inject SQL connection components into global namespace """
#     cnxn = {}
#     cnxn["engine"] = create_engine(make_url(DATABASE_URL_PARAMS))
#     cnxn["session_factory"] = sessionmaker(bind=cnxn["engine"])
#     cnxn["Session"] = scoped_session(cnxn["session_factory"])
#     cnxn["Base"] = declarative_base(bind=cnxn["engine"], cls=DeferredReflection)
#     globals().update(cnxn)
#     return cnxn


# connect_db()


# def prepare():
#     try:
#         Base.prepare(Base.metadata.bind)
#     except Exception as e:
#         logger.exception(e)
#         raise e


# # @listens_for(session_factory, "pending_to_persistent")
# # @listens_for(session_factory, "deleted_to_persistent")
# # @listens_for(session_factory, "detached_to_persistent")
# # @listens_for(session_factory, "loaded_as_persistent")
# # def detect_all_persistent(session, instance):
# #     print("object is now persistent: %s" % instance)


# class Operator(GenericTable, Base):
#     """Operator table"""

#     __tablename__ = OPERATOR_TABLENAME
#     __table_args__ = (
#         Column(
#             "id",
#             Integer(),
#             nullable=False,
#             default=Sequence(f"id_{OPERATOR_TABLENAME}", start=1, increment=1),
#         ),
#         Column("name", String(length=255), primary_key=True, nullable=False),
#         Column("alias", String(length=50)),
#         Column("confidence", Integer()),
#         Column("method", String(length=50)),
#         Column("source", String(length=50)),
#         Column("created_at", DateTime(), nullable=False, default=datetime.utcnow),
#         Column("updated_at", DateTime(), nullable=False, default=datetime.utcnow),
#         {"schema": DATABASE_SCHEMA},
#     )
#     session = Session()

#     def __repr__(self):
#         return f"Operator: {self.name}: {self.alias} ({self.confidence})"


# # FIXME: Re-add calculated columns
# class FracSchedule(GenericTable, Base):

#     __tablename__ = FRAC_SCHEDULE_TABLENAME
#     __table_args__ = (
#         Column(
#             "id",
#             Integer(),
#             nullable=False,
#             default=Sequence(f"id_{FRAC_SCHEDULE_TABLENAME}", start=1, increment=1),
#         ),
#         Column("api14", String(length=14), primary_key=True, nullable=False),
#         Column("api10", String(length=10)),
#         Column("operator", String(length=255)),
#         Column("operator_alias", String(length=50)),
#         Column("wellname", String(length=100)),
#         Column("fracstartdate", Date()),
#         Column("fracenddate", Date()),
#         Column("tvd", Integer()),
#         Column("shllat", Float()),
#         Column("shllon", Float()),
#         Column("bhllat", Float()),
#         Column("bhllon", Float()),
#         Column("created_at", DateTime(), nullable=False, default=datetime.utcnow),
#         Column("updated_at", DateTime(), nullable=False, default=datetime.utcnow),
#         {"schema": DATABASE_SCHEMA},
#     )
#     session = Session()

#     def __repr__(self):
#         return f"FracSchedule: {self.api14}: {self.operator_alias} | {self.operator}"


# # create table frac_schedule
# # (
# # 	id int identity,
# # 	api14 String(14) not null
# # 		constraint pk_frac_schedules_api
# # 			primary key,
# # 	api10 String(10),
# # 	operator String(100),
# # 	operator_alias String(50),
# # 	wellname String(100),
# # 	fracstartdate date,
# # 	fracenddate date,
# # 	tvd int,
# # 	shllat float,
# # 	shllon float,
# # 	bhllat float,
# # 	bhllon float,
# # 	days_to_fracstartdate as datediff(day,getdate(),[fracstartdate]),
# # 	days_to_fracenddate as datediff(day,getdate(),[fracenddate]),
# # 	status as case when datediff(day,getdate(),[fracstartdate])>0 then 'Planned' when datediff(day,getdate(),[fracenddate])>=0 then 'In-Progress' when datediff(day,getdate(),[fracenddate])>(-30) then 'Completed in Last 30 Days' when datediff(day,getdate(),[fracenddate])>(-60) then 'Completed in Last 60 Days' when datediff(day,getdate(),[fracenddate])>(-90) then 'Completed in Last 90 Days' when datediff(day,getdate(),[fracenddate])<=(-90) then 'Past Completion'  end,
# # 	updated datetime default getdate(),
# # 	inserted datetime default getdate() not null,
# # 	shl as case when [shllon] IS NOT NULL AND [shllat] IS NOT NULL then [GEOMETRY]::Point([shllon],[shllat],4326)  end,
# # 	bhl as case when [bhllon] IS NOT NULL AND [bhllat] IS NOT NULL then [GEOMETRY]::Point([bhllon],[bhllat],4326)  end,
# # 	stick as case when [shllon] IS NOT NULL AND [shllat] IS NOT NULL AND [bhllon] IS NOT NULL AND [bhllat] IS NOT NULL then [Geometry]::STGeomFromText('LINESTRING ('+CONVERT([String],[shllon])+' '+CONVERT([String],[shllat])+', '+CONVERT([String],[bhllon])+' '+CONVERT([String],[bhllat])+')',4326)  end
# # )
# # go


# def nullloads(geom):
#     if not pd.isna(geom):
#         if isinstance(geom, BaseGeometry):
#             return geom
#         return shapely.wkt.loads(geom)


# def to_wkt(geom):
#     if not pd.isna(geom):
#         return geom.wkt


# def frame_to_db(df: str, table: Table = FracSchedule):

#     if df is None:
#         logger.info("No data to load in DataFrame.")
#         return None

#     drop = ["crs", "nan", pd.np.nan]

#     if LOAD_GEOMETRY:

#         if "geometry" in df.columns:
#             g = GeoDataFrame(
#                 df,
#                 geometry=df.geometry.apply(nullloads),
#                 crs={"init": "epsg:" + str(WGS84)},
#             )

#             g["wkt"] = g.geometry.apply(to_wkt)

#             g = g.rename(columns={"wkt": "geometry"})
#         else:
#             g = df
#     else:
#         drop += ["geometry"]
#         g = df

#     for colname in drop:
#         try:
#             g = g.drop(columns=[colname])
#         except Exception as e:
#             logger.debug(f"{e}")

#     t0 = time.time()
#     # return g
#     table.merge_records(g, print_rec=False)
#     table.persist()
#     logger.info(
#         f"ORM merge_records(): Total time for {str(len(g))} records ({time.time() - t0:.2f}) secs"
#     )


# def recreate_database():
#     Base.metadata.drop_all()
#     Base.metadata.create_all()


# if __name__ == "__main__":

#     fs = FracSchedule
