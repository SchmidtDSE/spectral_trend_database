""" project paths/constants

License:
    BSD, see LICENSE.md
"""
from pathlib import Path


#
# PATHS/PROJECT
#
ROOT_MODULE = 'spectral_trend_database'
ROOT_DIR = Path(__file__).resolve().parent.parent
CONFIG_DIR = f'{ROOT_DIR}/config'
SPECTRAL_INDEX_DIR = f'{CONFIG_DIR}/spectral_indices'
NAMED_QUERY_DIR = f'{CONFIG_DIR}/named_queries'


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


#
# PRINTING/LOGGING
#
INFO_TYPES = ['info', 'warning', 'error']
