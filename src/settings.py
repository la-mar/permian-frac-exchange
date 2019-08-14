""" Settings module """

import logging
from calendar import month_name
from datetime import datetime
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()


""" Optional Pandas display settings"""
pd.options.display.max_rows = None
pd.set_option('display.float_format', lambda x: '%.2f' % x)
pd.set_option('large_repr', 'truncate')
pd.set_option('precision',2)

""" Logging """
LOG_CONFIG_PATH = './config/logging.yaml'
LOGLEVEL = logging.DEBUG

ENV_NAME = os.getenv('ENV_NAME', 'UNKNOWN')

""" Inputs """
OPERATOR_INDEX_SOURCE = os.getenv('FSEC_OI_SRC', 'file')

""" Outputs """
TO_CSV = os.getenv('FSEC_TO_CSV', True)
TO_DATABASE = os.getenv('FSEC_TO_DATABASE', False)
CSV_OUTPUT_PATH_TEMPLATE = './output/results-{dt}.csv'

""" API keys """
SENTRY_KEY = os.getenv('FSEC_SENTRY_KEY', None)
SENTRY_LEVEL = logging.WARNING
SENTRY_EVENT_LEVEL = logging.WARNING


""" File Paths """
DATAPATH = './data'
ALIASPATH = './config/column_aliases.yaml'
UNKNOWNPATH = './config/unknown_names.yaml'
CONFIGPATH = './config/config.yaml'
OPERATORPATH = './config/operators.json'


""" --------------- Sqlalchemy --------------- """

""" Database connection string

    Ex:
        MSSQL: 'mssql+pymssql://SERVER_NAME/DATABASE_NAME'


 """
DATABASE_URI = os.path.join(os.getenv('FSEC_DATABASE_URI',''), os.getenv('FSEC_DATABASE_NAME',''))
OPERATOR_TABLE = 'operator'
FRAC_SCHEDULE_TABLE = 'frac_schedule'
LOAD_GEOMETRY = False

""" Exclude these fields from ORM operations since they are managed by the database. """
EXCLUSIONS = ['updated',
              'inserted',
              'id',
              'days_to_fracstartdate',
              'days_to_fracenddate',
              'status']

""" Parser filename exclusions:
        Tokens in this list will be ignored when parsing file name.
        If the parser class is not finding the correct operator name in a file's name,
        adding elements to this list can help the parser find the correct name."""
EXCLUDE_TOKENS = list(map(str.lower, ['mb',
                                      'pb',
                                      'frac',
                                      'schedule',
                                      '3monthfracschedule'] \
                                      + list(month_name)))

""" Mapping of column names to the desired datatype. Pandas will cast columns in
    frac_schedule dataframes into these data types."""
COLUMN_DTYPES = {
    'operator': str,
    'operator_alias': str,
    'wellname': str,
    'api14': str,
    'api10': str,
    'fracstartdate': datetime,
    'fracenddate': datetime,
    'shllat': float,
    'shllon': float,
    'bhllat': float,
    'bhllon': float,
    'tvd': float,
    'crs': str,
    'geometry': object
}

""" Column names of a DataFrame containing frac_schedules """
COLUMN_NAMES = list(COLUMN_DTYPES.keys())

""" Exception Message Templates"""
__MSG_PARSER_CORRUPT__ = 'Parser({op_name}) is corrupt - bypassing method call.'
__MSG_PARSER_ERROR__ = 'Error in Parser({op_name}) -- {e}'
__MSG_PARSER_PARSING__ = 'Error parsing value ({v}) in Parser({op_name}) -- {e}'
__MSG_PARSER_COLLECTION_STATUS__ = 'Executing {func_name} on ParserCollection({pc_name})'

""" --------------- Geographic Constants --------------- """

""" EPSG code for WGS84 """
WGS84 = 4326

""" EPSG code for NAD27 """
NAD27 = 4267

""" EPSG code for NAD27 State Plane projection """
NAD27SP = 32039


















