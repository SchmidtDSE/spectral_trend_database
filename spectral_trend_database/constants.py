""" project paths/constants

NOTE:
    constants (excluding "FIXED" constants) can be overwritten
    using yaml file at `config/user.yaml`

License:
    BSD, see LICENSE.md
"""
from pathlib import Path


#
# FIXED (do not overwrite with `config/user.yaml`)
#
ROOT_MODULE = 'spectral_trend_database'
INFO_TYPES = ['info', 'warning', 'error']
SAFE_NAN_VALUE = -99999


#
# PATHS/PROJECT
#
PROJECT_DIR = Path(__file__).resolve().parent.parent
PARENT_DIR = PROJECT_DIR.parent
CONFIG_DIR = f'{PROJECT_DIR}/config'
USER_CONFIG_PATH = f'{PROJECT_DIR}/config/user.yaml'
SPECTRAL_INDEX_DIR = f'{CONFIG_DIR}/spectral_indices'
NAMED_QUERY_DIR = f'{CONFIG_DIR}/named_queries'


#
# DATA
#
LSAT_BANDS = [
    'blue',
    'green',
    'red',
    'nir',
    'swir1',
    'swir2'
]
DATE_COLUMN = 'date'
DATETIME_NS = 'datetime64[ns]'
DATETIME_MS = 'datetime64[ms]'


#
# DEFAULTS
#
DEFAULT_QUERY_CONFIG = 'v1'
DEFAULT_SPECTRAL_INDEX_CONFIG = 'v1'
DEFAULT_LOCATION = 'US'
DEFAULT_TIMEOUT = 30


#
# GCP
#
URI_PREFIX = 'gs://'
URL_PREFIX = 'https://storage.googleapis.com/'