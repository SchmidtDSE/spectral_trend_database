""" config hander

DESCRIPTION:
    provides an interface to access constants
    defined in `spectral_trend_database/constants.py` or
    user config defined in `config/user.yaml`.

    values can be accessed through `c.get(key, default)` if default is
    required. otherwise use dot-notaion (c.key) or superscript (c[key])

    note: the user config overwrites the constants.

USAGE:
    ```python
    from spectral_trend_database.config import config as c

    print(c.some_key_in_user_config_yaml)
    print(c.some_constant_in_spectral_trend_database_constants)
    ```

License:
    BSD, see LICENSE.md
"""
from typing import Any, Optional, Union
from types import ModuleType
from collections import UserDict
from pathlib import Path
from spectral_trend_database import constants
from spectral_trend_database import utils


#
# CONSTANTS
#
_NOT_FOUND = '__NOT_FOUND'
_KEY_ERROR = '"{}" not in config or constants'
_PROTECTED_ERROR = '"{}" is protected and may not be included included in user-config.'
PROTECTED_KEYS = [
    'ROOT_MODULE',
    'INFO_TYPES',
    'SAFE_NAN_VALUE',
    'DATETIME_NS',
    'DATETIME_MS',
    'YYYY_MM_DD_FMT']


#
# HANDLER
#
class ConfigHandler(UserDict):

    def __init__(
            self,
            path: str,
            constants: Optional[ModuleType] = constants,
            protected_keys: Optional[list[str]] = PROTECTED_KEYS) -> None:
        self.constants = constants
        self.config = utils.read_yaml(path, safe=True) or {}
        self.protected_keys = protected_keys
        self._check_protected_keys()

    def update(self, config: [str, dict]) -> None:
        if isinstance(config, str):
            config = utils.read_yaml(path, safe=True)
        self.config.update(config)
        self._check_protected_keys()

    def get(self, key: str, default: Any = None):
        value = self.config.get(key, _NOT_FOUND)
        if value == _NOT_FOUND:
            try:
                value = getattr(self.constants, key)
            except:
                value = default
        return value

    def __getitem__(self, key):
        value = self.get(key, _NOT_FOUND)
        if value == _NOT_FOUND:
            error = utils.message(
                _KEY_ERROR.format(key),
                'config',
                level=None,
                return_str=True)
            raise KeyError(error)
        return value

    def __getattr__(self, key):
        return self.__getitem__(key)

    #
    # PRIVATE
    #
    def _check_protected_keys(self):
        if self.protected_keys:
            for key in self.protected_keys:
                config_keys = self.config.keys()
                if key in config_keys:
                    error = utils.message(
                        _PROTECTED_ERROR.format(key),
                        'config',
                        level=None,
                        return_str=True)
                    raise ValueError(error)


config = ConfigHandler(constants.USER_CONFIG_PATH)
