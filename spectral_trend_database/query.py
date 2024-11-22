""" utility methods

License:
    BSD, see LICENSE.md
"""
from typing import Optional, Union, Any, Sequence, TypeAlias, Literal
import re
from copy import deepcopy
import pandas as pd
from google.cloud import bigquery as bq
from spectral_trend_database.config import config as c
from spectral_trend_database import utils


#
# TYPES
#
JOINS: TypeAlias = Literal[
    'LEFT',
    'RIGHT',
    'INNER',
    'OUTER',
    'left',
    'right',
    'inner',
    'outer']
SEQUENCE_STRS: TypeAlias = Union[Sequence[str], str]


#
# CLASSES
#
OPERATOR_SUFFIX: str = 'op'
DEFAULT_OPERATOR: str = '='


class QueryConstructor(object):
    """ class for constructing SQL queries

    Usage:

        ```python
        sqlc = QueryConstructor('table1', using='sample_id')
        sqlc.select('sample_id', 'c1', 'c2', c3='c_three')
        sqlc.join('table2', how='right', on='c2')
        sqlc.join('table3', on=('c2', 'c3p2'))
        sqlc.join('table4')
        sqlc.where(c2=123456)
        sqlc.where('table3', c_three=1234, c_three_op='<=')
        sqlc.limit(100)
        print(sqlc.sql())
        ```

        ```sql
        SELECT sample_id, c1, c2, c3 as c_three FROM table1
        RIGHT JOIN table2 ON table1.c2 = table2.c2
        LEFT JOIN table3 ON table1.c2 = table3.c3p2
        LEFT JOIN table4 USING (sample_id)
        WHERE table1.c2 = 123456 AND table3.c_three <= 1234
        LIMIT 100
        ```
    """
    @classmethod
    def from_config(cls, config):
        """ generate QueryConstructor instance from config file
        """
        config = deepcopy(config)
        init = config.get('init')
        lim=config.get('limit')
        sqlc = cls(**init)
        for _cfig in cls._args_as_list(config, 'select', []):
            args, kwargs = cls._process_args_kwargs(_cfig)
            sqlc.select(*args, **kwargs)
        for _cfig in cls._args_as_list(config, 'join', []):
            table = _cfig.pop('table')
            sqlc.join(
                table,
                *cls._as_list(_cfig.get('using', [])),
                how=_cfig.get('how'),
                on=_cfig.get('on'),
                join_table=_cfig.get('join_table'))
        for _cfig in cls._args_as_list(config, 'where', []):
            table = _cfig.pop('table')
            sqlc.where(table, **_cfig)
        for _cfig in cls._args_as_list(config, 'append', []):
            sqlc.append(_cfig)
        if lim:
            sqlc.limit(lim)
        return sqlc


    def __init__(self,
            table: str,
            how: JOINS='LEFT',
            on: Optional[SEQUENCE_STRS]=None,
            using: Optional[SEQUENCE_STRS]=None):
        """
        Args:
            table (str): table-name
            how (JOINS='LEFT'):
                default type of join [LEFT, RIGHT, INNER, OUTER]
                note: lower case allowed
            on (Optional[SEQUENCE_STRS]=None):
                string or list of strings to `JOIN ... ON`
            using (Optional[SEQUENCE_STRS]=None):
                string or list of strings to `JOIN ... USING`
                note: <using> takes precedence over <on>
        """
        self.reset()
        self._table = table
        self._default_how = how
        self._default_on = self._as_list(on)
        self._default_using = self._as_list(using)


    def select(self, *columns: str, **columns_as) -> None:
        """ add select columns

        Note: if not called select will revert to `SELECT *`

        Args:
            *columns (str): names of columns to include
            **columns_as (str): key (name) value (as-name) pairs for renaming columns

        Usage:
            ```python
            sqlc.select('column_1', 'column_2', column_3='c3')
            ...
            sqlc.sql() # => 'SELECT column_1, column_2, column_3 as c3 FROM ...'
            ```
        """
        self._select_list += list(columns) + [f'{k} as {v}' for k, v in columns_as.items()]


    def join(self,
            table: str,
            *using: str,
            how: Optional[str]=None,
            on: Optional[Union[Sequence, str]]=None,
            join_table: Optional[str]=None) -> None:
        """ add join

        TODO: manage joins on with different column names

        When constructing JOINs we prioritze <using> over <on>. Namely:
            1. If <using> use <using>
            2. Else If <on> use <on>
            3. Else If <self._default_using> use <self._default_using>
            4. Else If <self._default_on> use <self._default_on>

        Args:
            table (str): table name to join
            *using (str):
                column names for `JOIN ... USING`
                uses default <using> set in initialization method if None
            how (Optional[str]=None):
                type of join [LEFT, RIGHT, INNER, OUTER]
                uses default <how> set in initialization method if None
            on (Optional[Union[Sequence, str]]=None):
                string or list of strings to `JOIN ... ON`
                uses default <on> set in initialization method if None
            join_table (Optional[str]=None):
                name of table to join with
                uses <table> passed in initialization method if None
        """
        join_element = self._join_element(table, join_table, how, using, on)
        self._join_list.append(join_element)


    def where(self, table: Optional[str]=None, **kwargs: Union[str, int, float]) -> None:
        """ add where statement

        Sets where statement through key value pairs.

        if kwarg ends in "_op" it is used as the comparison operator
        (otherwise the operator) is "=".

        for example:
            `year=2010` =>  "... WHERE year=2010", but
            `year=2010, year_op="<"` => "... WHERE year<2010"

        Args:
            table (str): table name used in where statement
            **kwargs (Union[str, int, float]):
                key value pairs for where statement
                operators set using "_op" as described above

        Usage:
            ```python
            sqlc.where('table2', year=2020, year_op='<', sample_id='asd23ragwd')
            ...
            sqlc.sql() # => '... WHERE table2.year < 2020 AND table2.sample_id = "asd23ragwd"'
            ```
        """
        if not table:
            table = self._table
        keys_values = [
            (k, v) for k, v in kwargs.items()
            if not re.search(f'_{OPERATOR_SUFFIX}$', k)]
        for k, v in keys_values:
            self._where_list.append({
                'key': k,
                'table': table,
                'value': self._sql_query_value(v),
                'operator': kwargs.get(f'{k}_{OPERATOR_SUFFIX}', DEFAULT_OPERATOR)})


    def append(self, *values: str) -> None:
        """ append strings seperated by a space to end of sql statement

        This has been included to allow users to add explicit SQL statements which
        may not be possible with current API.

        Args:
            *values (str):
                append each element of <values> to end of sql statement
                but before LIMIT
        """
        self._append_list += values


    def limit(self, max_rows: Optional[int]=None) -> None:
        """ limit number of rows

        Args:
            max_rows (Optional[int]=None): if exists limit number of rows
        """
        self._limit = max_rows


    def sql(self, force: bool=False) -> str:
        """ get sql statement

        This will construct (if not yet constructed or `force=True`) the sql statment and return
        the string.

        Args:
            force (bool=False): if true (re)construct sql statement even if it already exists
        Returns:
            (str) SQL statement
        """
        if force or (not self._sql):
            self._sql = self._construct_sql()
        return self._sql


    def reset(self) -> None:
        """ resets/initializes instance """
        self._sql = None
        self._select_list = []
        self._join_list = []
        self._where_list = []
        self._append_list = []
        self._limit = None


    #
    # INTERNAL (STATIC & CLASS)
    #
    @staticmethod
    def _process_args_kwargs(value):
        if isinstance(value, list):
            args = value
            kwargs = {}
        elif isinstance(value, dict):
            args = []
            kwargs = value
        elif isinstance(value, tuple) and (len(value)==2):
            args, kwargs = value
        else:
            args = [value]
            kwargs = {}
        return args, kwargs


    @staticmethod
    def _as_list(value):
        if value and (not isinstance(value, list)):
            value = [value]
        return value


    @classmethod
    def _args_as_list(cls, config, key, default=None):
        value = config.get(key, default)
        return cls._as_list(value)


    #
    # INTERNAL (INSTANCE)
    #
    def _construct_sql(self) -> str:
        """ construct (and return) the sql-statement
        """
        if self._select_list:
            self._select = 'SELECT ' + ', '.join(self._select_list)
        else:
            self._select = 'SELECT *'
        self._select += f' FROM {self._table}'
        self._join = ' '.join(self._join_list)
        sql_statement = self._select  + ' ' + self._join
        if self._where_list:
            where_statements = [self._process_where(**kw) for kw in self._where_list]
            sql_statement += ' WHERE ' + ' AND '.join(where_statements)
        if self._append_list:
            sql_statement += ' ' + ' '.join(self._append_list)
        if self._limit:
            sql_statement += f' LIMIT {self._limit}'
        return sql_statement


    def _join_element(self, table, join_table, how, using, on) -> str:
        """ construct JOIN-statement part """
        if not join_table:
            join_table = self._table
        if not how:
            how = self._default_how
        _statement = f'{how.upper()} JOIN {table}'
        if not (using or on):
            if self._default_using:
                using = self._default_using
            else:
                on = self._default_on
        if using:
            _statement += f' USING ({", ".join(using)})'
        elif on:
            on = self._as_list(on)
            on = [self._process_on(v, table, join_table) for v in on]
            _statement += ' ON ' + ' AND '.join(on)
        else:
            raise ValueError('statement required')
        return _statement


    def _process_where(self, table, key, value, operator) -> str:
        """ construct WHERE-statement part """
        return f'{table}.{key} {operator} {value}'


    def _process_on(self, value, table, join_table) -> str:
        """ construct ON-statement part """
        if not isinstance(value, (list, tuple)):
            value = value, value
        return f'{join_table}.{value[0]} = {table}.{value[1]}'


    def _sql_query_value(self, value: Union[str, int, float]) -> Union[str, int, float]:
        """ safe query value
        if value not int or float return in quotes
        """
        if isinstance(value, (int, float)):
            return value
        else:
            return f'"{value}"'


#
# METHODS
#
def queries(config: Union[dict[str, Any], str] = c.DEFAULT_QUERY_CONFIG) -> list['str']:
    """ list of queries in config file

    Args:
        - config (Union[str,dict]=c.DEFAULT_QUERY_CONFIG):
            configuration dictionary containg sql-config with key <name>
            if (str):
                if re.search(r'(yaml|yml)$', <config>) loads yaml file with at <path config>
                else loads yaml at '<project-root>/config/named_queries/<config>.yaml'
    """
    if isinstance(config, str):
        if not re.search(r'(yaml|yml)$', config):
            config = f'{c.NAMED_QUERY_DIR}/{config}.yaml'
        config = utils.read_yaml(config)
    assert isinstance(config, dict)
    return list(config['queries'].keys())


def named_sql(
        name: Optional[str] = None,
        table: Optional[str] = None,
        select: Optional[str] = None,
        table_config: dict = {},
        config: Union[dict[str, Any], str] = c.DEFAULT_QUERY_CONFIG,
        limit: Optional[int] = None,
        **values) -> str:
    """ generate sql command from config file

    Consider the named queries config file:

    ```yaml
    # config/named_queries/example.yaml
    project: dse-regenag
    dataset: BiomassTrends
    defaults:
      how: LEFT
      select: '*'
      using: sample_id
    queries:
      scym_raw_all:
        table: SAMPLE_POINTS
        join:
          - table: SCYM_YIELD
          - table: LANDSAT_RAW_MASKED
            using: sample_id, year
    ```

    - The gcp project and dataset are set using the `project`/`dataset` values.
    - The `defaults` dict gives default values to add to each query (if the query) doesn't
      explicitly provide them. For example the above yaml says to always use a
      LEFT join, however if in one of the named `queries` (see below) contains `how: RIGHT`
      a RIGHT join will be used instead.  Similarly this will by default use
      `SELECT * ...`, but if a query contains `select: a, b, c` the query will be
      `SELECT a, b, c ...`.
    - `queries` is a dictionary with all the named queries. We start my adding creating a
      select statement 'SELECT {select} FROM {table}', where "{}" indicate the value
      subtracted from the named-query dict or the defaults dict. Then we sequentially loop
      over the join list using the {table} and {join} values.

    Examples:

        `named_sql('scym_raw_all')` will output

        ```sql
        SELECT * FROM `dse-regenag.BiomassTrends.SAMPLE_POINTS`
        LEFT JOIN `dse-regenag.BiomassTrends.SCYM_YIELD` USING (sample_id)
        LEFT JOIN `dse-regenag.BiomassTrends.LANDSAT_RAW_MASKED` USING (sample_id, year)
        ```

        We can also add a `where` key:

        ``` yaml
          scym_raw_for_1999:
            table: SAMPLE_POINTS
            join:
              - table: SCYM_YIELD
              - table: LANDSAT_RAW_MASKED
                using: sample_id, year
            where:
            - key: year
              table: SCYM_YIELD
              value = 1999
        ```

        now `named_sql('scym_raw_for_1999')` will output

        ```sql
        SELECT * FROM `dse-regenag.BiomassTrends.SAMPLE_POINTS`
        LEFT JOIN `dse-regenag.BiomassTrends.SCYM_YIELD` USING (sample_id)
        LEFT JOIN `dse-regenag.BiomassTrends.LANDSAT_RAW_MASKED` USING (sample_id, year)
        WHERE `dse-regenag.BiomassTrends.SCYM_YIELD`.year = 1999
        ```

        More intresting is

        ``` yaml
          scym_raw_for_year:
            table: SAMPLE_POINTS
            join:
              - table: SCYM_YIELD
              - table: LANDSAT_RAW_MASKED
                using: sample_id, year
            where:
            - key: year
              table: SCYM_YIELD
        ```

        now `named_sql('scym_raw_for_year')` will throw an error because the value
        is not specified. However, you can pass a keyword value for the `year`
        to the `named_sql` method.

        `named_sql('scym_raw_for_year', year=2020)` will output

        ```sql
        SELECT * FROM `dse-regenag.BiomassTrends.SAMPLE_POINTS`
        LEFT JOIN `dse-regenag.BiomassTrends.SCYM_YIELD` USING (sample_id)
        LEFT JOIN `dse-regenag.BiomassTrends.LANDSAT_RAW_MASKED` USING (sample_id, year)
        WHERE `dse-regenag.BiomassTrends.SCYM_YIELD`.year = 2020
        ```

        We can also query a single Table without a named query:

        `named_sql(table='raw_indices_v1', year=2010, limit=100)` will output:

        ```sql
        SELECT * FROM `dse-regenag.BiomassTrends.RAW_INDICES_V1`
        WHERE `dse-regenag.BiomassTrends.RAW_INDICES_V1`.year = 2010
        LIMIT 100
        ```

        Note that the table-name is coerced to all upper case.


    Args:
        name (Optional[str]): name of preconfigured config file
        table (Optional[str]):
            (required if name is None) table-name: queries a
            single table with optional `WHERE` clause added through
            `values` kwargs.
        config (Union[str,dict]=c.DEFAULT_QUERY_CONFIG):
            configuration dictionary containg sql-config with key <name>
            if (str):
                if re.search(r'(yaml|yml)$', <config>) loads yaml file with at <path config>
                else loads yaml at '<project-root>/config/named_queries/<config>.yaml'
        limit (int=None):
            if exits add "LIMIT <limit>" to end of SQL call
        **values:
            values for where clause (see usage above).

            note: if kwarg ends in "_op" it is used as the comparison operator
            (otherwise the operator) is "=".

            for example:
                `year=2010` =>  "... WHERE year=2010", but
                `year=2010, year_op="<"` => "... WHERE year<2010"


    Returns:
        (str) sql command
    """
    if isinstance(config, str):
        if not re.search(r'(yaml|yml)$', config):
            config = f'{c.NAMED_QUERY_DIR}/{config}.yaml'
        config = utils.read_yaml(config)
    assert isinstance(config, dict)
    project = config.get('project')
    dataset = config.get('dataset')
    if dataset:
        if project:
            dataset = f'{project}.{dataset}'
        dataset += '.'
    else:
        dataset = ''
    if name:
        cfig = {**config.get('defaults', {}), **config['queries'][name]}
    elif table:
        table = table.upper()
        cfig = config.get('defaults', {})
        cfig.update(table_config)
        cfig['table'] = table
        cfig['where'] = []
        keys_values = [(k, v) for k, v in values.items() if not re.search('_op$', k)]
        for k, v in keys_values:
            where_config = {}
            where_config['key'] = k
            where_config['table'] = table
            where_config['value'] = _sql_query_value(v)
            where_config['operator'] = values.get(f'{k}_op', "=")
            cfig['where'].append(where_config)
    else:
        err = (
            'spectral_trend_database.query.named_sql: '
            'either <name> or <table> must be non-null'
        )
        raise ValueError(err)
    sql = f"SELECT {select or cfig['select']} FROM `{dataset}{cfig['table']}`"
    for join in cfig.get('join', []):
        jcfig = {**cfig, **join}
        sql += f" {jcfig['how']} JOIN `{dataset}{jcfig['table']}`"
        sql += f" USING ({jcfig['using']})"
    for i, where in enumerate(cfig.get('where', [])):
        table = where['table']
        key = where['key']
        op = where.get('operator', '=')
        try:
            value = where['value']
        except:
            value = values[key]
        if i:
            where_sql = ' AND'
        else:
            where_sql = ' WHERE'
        sql += f'{where_sql} `{dataset}{table}`.{key} {op} {value}'
    if limit:
        sql += f' LIMIT {limit}'
    return sql


def run(
        name: Optional[str] = None,
        table: Optional[str] = None,
        config: Union[dict[str, Any], str] = c.DEFAULT_QUERY_CONFIG,
        limit: Optional[int] = None,
        sql: Optional[str] = None,
        print_sql: bool = False,
        project: Optional[str] = None,
        client: Optional[bq.Client] = None,
        to_dataframe: bool = True,
        **values) -> Union[bq.QueryJob, pd.DataFrame]:
    """ queries bigquery

    Executes a bigquery query either through an explicit sql string, using the
    `sql` arg, or by creating a sql-string using the `named_sql` method above.

    Args:
        name (Optional[str]): name of preconfigured config file
        table (Optional[str]):
            (required if name is None) table-name: queries a
            single table with optional `WHERE` clause added through
            `values` kwargs.
        config (Union[str,dict]=c.DEFAULT_QUERY_CONFIG):
            configuration dictionary containg sql-config with key <name>
            if (str):
                if re.search(r'(yaml|yml)$', <config>) loads yaml file with at <path config>
                else loads yaml at '<project-root>/config/named_queries/<config>.yaml'
        limit (int=None):
            if exits add "LIMIT <limit>" to end of SQL call
        sql (str=None): if <name> not provided, explicit sql command to use in query
        print_sql (bool = False): if true print sql-string before executing query
        project (str=None): gcp project name
        client (bq.Client=None):
            instance of bigquery client
            if None a new one will be instantiated
        **values:
            values for where clause (see usage above)

    Returns:
        (str) sql command
    """
    if client is None:
        client = bq.Client(project=project)
    if name or table:
        sql = named_sql(name=name, table=table, config=config, limit=limit, **values)
    assert sql is not None
    if print_sql:
        utils.message(sql, 'query', 'run')
    resp = client.query(sql)
    if to_dataframe:
        return resp.to_dataframe()
    else:
        return resp


#
# INTERNAL
#
def _sql_query_value(value: Union[str, int, float]):
    if isinstance(value, (int, float)):
        return value
    else:
        return f'"{value}"'
