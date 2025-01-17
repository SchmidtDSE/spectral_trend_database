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
from spectral_trend_database import types


#
# CONSTANTS
#
OPERATOR_SUFFIX: str = 'op'
DEFAULT_OPERATOR: str = '='
TABLE_KEYS: list[str] = ['table', 'join_table']


#
# CLASSES
#
class QueryConstructor(object):
    """ class for constructing SQL queries

    Usage:

    Constructs SQL queries

    using python:

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

    or using a config file:

    ```yaml
    init:
        table: table1
        using: sample_id
        table_escape: null
    select:
        - c1, c2
        - c3: as c_three
    join:
        - table: table2
          how: right
          on: c2
        - table: table3
          on:
            - c2
            - c3p2
        - table: table 4
    where:
        - c2: 123456
        - table: table3
          c_three: 1234
          c_three_op: '<='
    limit: 100
    ```

    and then:

    ```python
    sqlc = QueryConstructor.from_config('table1', using='sample_id')
    print(sqlc.sql())
    ```

    resulting in

    ```sql
    SELECT sample_id, c1, c2, c3 as c_three FROM table1
    RIGHT JOIN table2 ON table1.c2 = table2.c2
    LEFT JOIN table3 ON table1.c2 = table3.c3p2
    LEFT JOIN table4 USING (sample_id)
    WHERE table1.c2 = 123456 AND table3.c_three <= 1234
    LIMIT 100
    ```

    """
    #
    # CLASS METHODS
    #
    @classmethod
    def from_config(cls, config):
        """ generate QueryConstructor instance from config file

        Constructs SQL query sing a config dict where each of the keys are methods on
        QueryConstructor, where the values are dicts or list of dicts for the arguments
        of the QueryConstructor methods.

        Note the QueryConstructor.__init__ function simply uses the key "init".

        EXAMPLE:

        ```yaml
        # example.yaml
        init:
            table: table1
            using: sample_id
            table_escape: null
        join:
            - table: table2
            - table: table3
            - table: table3
              using:
                - sample_id
                - year
        where:
            - year: 2020
            - year_op: '<='
        ```

        ```python
        config = load_yaml('example.yaml')
        sqlc = QueryConstructor.from_config(config)
        print(sqlc.sql())
        ```

        ```sql
        SELECT * FROM table1
        LEFT JOIN table2 USING (sample_id)
        LEFT JOIN table3 USING (sample_id)
        LEFT JOIN table3 USING (sample_id, year)
        WHERE table1.year <= 2020
        ```

        Notes:

        - JOIN note on "on":
            PyYaml converts "on" to True (even for keys). i've allowed
            for "on"-key in 3 ways:
                1. make string explicit with quotes. ie `'on': sample_id`
                2. use "on_columns": ie `on_columns: sample_id`
                3. leave it as on. the config will show up `True: sample_id`, but
                   within a join this will be converted to "on".
        - WHERE note on "_op":
            As discussed below: in the `where` method if kwarg ends in "_op" it is
            used as the comparison operator (otherwise the operator) is "=".

            for example:
                `year=2010` =>  "... WHERE year=2010", but
                `year=2010, year_op="<"` => "... WHERE year<2010"

            this will be problamatic in a yaml file for some operators. escape these values
            with quotes: ie `year_op: '<='`
        """
        config = deepcopy(config)
        init = config.get('init')
        lim = config.get('limit')
        qc = cls(**init)
        for _cfig in cls._args_as_list(config, 'select', []):
            args, kwargs = cls._process_args_kwargs(_cfig)
            qc.select(*args, **kwargs)
        for _cfig in cls._args_as_list(config, 'join', []):
            table = _cfig.pop('table')
            qc.join(
                table,
                *cls._as_list(_cfig.get('using', [])),
                how=_cfig.get('how'),
                on=_cfig.get('on') or _cfig.get('on_columns') or _cfig.get(True),
                join_table=_cfig.get('join_table'))
        for _cfig in cls._args_as_list(config, 'where', []):
            safe_table = [t for t in [_cfig.pop('table', None)] if t]
            qc.where(*safe_table, **_cfig)
        for _cfig in cls._args_as_list(config, 'append', []):
            qc.append(_cfig)
        if lim:
            qc.limit(lim)
        return qc

    #
    # PUBLIC
    #
    def __init__(self,
            table: str,
            table_prefix: Optional[str] = None,
            table_escape: Optional[str] = '`',
            how: types.JOINS = 'LEFT',
            on: Optional[types.STRINGS] = None,
            using: Optional[types.STRINGS] = None,
            uppercase_table: bool = True) -> None:
        """
        Args:

            table (str): table-name
            table_prefix (Optional[str] = None):
                prefix to be added to table/joint_table names.
                if <table_prefix> and '.' not in <table>/<joint_table>
                then <table>/<joint_table> => <table_prefix>.<table>/<joint_table>
            table_escape (Optional[str] = '`'):
                if exists escape table names using this value.
                ie if table_escape = '`': TABLE_NAME => `TABLE_NAME`
            how (types.JOINS='LEFT'):
                default type of join [LEFT, RIGHT, INNER, OUTER]
                note: lower case allowed
            on (Optional[types.STRINGS]=None):
                string or list of strings to `JOIN ... ON`
            using (Optional[types.STRINGS]=None):
                string or list of strings to `JOIN ... USING`
                note: <using> takes precedence over <on>
            uppercase_table (bool = True):
                if true apply `.upper()` to <table>. Note only used if <table>
                is non-null.
        """
        self.reset()
        self._default_how = how
        self._default_on = self._as_list(on)
        self._default_using = self._as_list(using)
        self._table_prefix = table_prefix
        self._table_escape = table_escape
        self._uppercase_table = uppercase_table
        self._table = self._table_name(table)

    def reset(self) -> None:
        """ resets instance """
        self._sql: Optional[str] = None
        self._select_list: list = []
        self._join_list: list = []
        self._where_list: list = []
        self._order_cols: list = []
        self._append_list: list = []
        self._limit: Optional[int] = None

    def select(self, *columns: str, table: Optional[str] = None, **columns_as) -> None:
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
        if table:
            columns = [f'{self._table_name(table)}.{col}' for col in columns]
            columns_as = {f'{self._table_name(table)}.{k}': v for k, v in columns_as.items()}
        self._select_list += list(columns) + [f'{k} as {v}' for k, v in columns_as.items()]

    def join(self,
            table: str,
            *using: str,
            how: Optional[str] = None,
            on: Optional[Union[Sequence, str]] = None,
            join_table: Optional[str] = None) -> None:
        """ add join

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
        table = self._table_name(table)
        join_table = self._table_name(join_table)
        join_element = self._join_element(table, join_table, how, using, on)
        self._join_list.append(join_element)

    def where(self, table: Optional[str] = None, **kwargs: Union[str, int, float]) -> None:
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
        table = self._table_name(table)
        keys_values = [
            (k, v) for k, v in kwargs.items()
            if not re.search(f'_{OPERATOR_SUFFIX}$', k)]
        for k, v in keys_values:
            op = kwargs.get(f'{k}_{OPERATOR_SUFFIX}', DEFAULT_OPERATOR)
            self._where_list.append({
                'key': k,
                'table': table,
                'value': self._sql_query_value(v, op=op),
                'operator': op})

    def where_in(self,
            table: Optional[str] = None,
            quote_escape: bool = True,
            **kwargs: Union[Sequence]) -> None:
        """ convinece wrapper for `.where()`, when construncting `WHERE ... IN` statements

        Sets where statement through key value pairs.

        Args:

            table (str): table name used in where statement
            quote_escape (bool = True): if False do not put qoutes around values
            **kwargs (Union[List]):
                key value-list pairs for where-in statement
                ie:
                    * year=[2002, 2004, 2006] gives

                        WHERE year IN ('2002', '2004', '2006')

                    * if quote_escape=False it gives

                        WHERE year IN (2002, 2004, 2006)
        """
        for key, values in kwargs.items():
            if quote_escape:
                values = [f"'{v}'" for v in values]
            else:
                values = [str(v) for v in values]
            values_str = f'({", ".join(values)})'
            self.where(**{
                'table': table,
                key: values_str,
                f'{key}_op': 'IN'})

    def orderby(self, *columns: str, table: Optional[str] = None, asc: bool = True) -> None:
        """ order by columns
        This has been included to allow users to add explicit SQL statements which
        may not be possible with current API.

        Args:

            *columns (str):
                columns to order by
            asc (bool = True):
                if true order ascending otherwise descending
        """
        self._order_cols.append([list(columns), self._table_name(table), asc])

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

    def limit(self, max_rows: Optional[int] = None) -> None:
        """ limit number of rows

        Args:

            max_rows (Optional[int]=None): if exists limit number of rows
        """
        self._limit = max_rows

    def sql(self, force: bool = False) -> str:
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
        elif isinstance(value, tuple) and (len(value) == 2):
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
    def _table_name(self, table):
        if table:
            if self._uppercase_table:
                parts = table.split('.')
                table = '.'.join(parts[:-1] + [parts[-1].upper()])
            if self._table_prefix and ('.' not in table):
                table = f'{self._table_prefix}.{table}'
        else:
            table = self._table
        if table and self._table_escape:
            rgx = f'{self._table_escape}.+{self._table_escape}$'
            if not re.search(rgx, table):
                table = f'{self._table_escape}{table}{self._table_escape}'
        return table

    def _construct_sql(self) -> str:
        """ construct (and return) the sql-statement
        """
        if self._select_list:
            self._select = 'SELECT ' + ', '.join(self._select_list)
        else:
            self._select = 'SELECT *'
        self._select += f' FROM {self._table}'
        sql_statement = self._select
        if self._join_list:
            self._join = ' '.join(self._join_list)
            sql_statement += ' ' + self._join
        if self._where_list:
            where_statements = [self._process_where(**kw) for kw in self._where_list]
            self._where = ' AND '.join(where_statements)
            sql_statement += ' WHERE ' + self._where
        if self._append_list:
            sql_statement += ' ' + ' '.join(self._append_list)
        if self._order_cols:
            _statements = []
            for cols_tbl_asc in self._order_cols:
                cnames = [f'{cols_tbl_asc[1]}.{n}' for n in cols_tbl_asc[0]]
                if cols_tbl_asc[2]:
                    _order = 'ASC'
                else:
                    _order = 'DESC'
                _statements.append(f' {_order}, '.join(cnames) + f' {_order}')
            sql_statement += ' ORDER BY ' + ', '.join(_statements)
        if self._limit:
            sql_statement += f' LIMIT {self._limit}'
        return sql_statement

    def _join_element(self, table, join_table, how, using, on) -> str:
        """ construct JOIN-statement part """
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
        if isinstance(value, str):
            value = [v.strip() for v in value.split(',')]
        nb_parts = len(value)
        if nb_parts == 2:
            v1, v2 = value
        elif nb_parts == 1:
            v1, v2 = value[0], value[0]
        else:
            err = (
                'query.QueryConstructor._process_on'
                'value must be tuple of len 2, or'
                'a string containg no more than one comma')
            raise ValueError(err)
        return f'{join_table}.{v1} = {table}.{v2}'

    def _sql_query_value(self, value: Union[str, int, float], op: str) -> Union[str, int, float]:
        """ safe query value
        if value not int or float or "on"-operator return in quotes
        """
        if isinstance(value, (int, float)) or (op.lower() == 'in'):
            return value
        else:
            return f'"{value}"'


#
# METHODS
#
def process_named_query_config(config: dict, query_name: Optional[str] = None) -> dict:
    """ Extracts named query from queries config

    Args:

        config (dict): queries-config as described above
        query_name (str):
            key for query in queries-config
            to extract: config['queries'][<query_name>]

    Returns:

        (dict) query-config that can be passed to
        QueryConstructor.from_config(...)
    """
    config = deepcopy(config)
    defaults = config.get('defaults', {})
    table_prefix = defaults.get('table_prefix')
    if not table_prefix:
        project = config.get('project')
        dataset = config.get('dataset')
        table_prefix = '.'.join([v for v in [project, dataset] if v])
    if query_name:
        config = config['queries'][query_name]
        config['init'] = {**defaults, **config.get('init', {})}
    config['init']['table_prefix'] = config['init'].get('table_prefix', table_prefix)
    return config


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
        config: Union[dict[str, Any], str] = c.DEFAULT_QUERY_CONFIG,
        limit: Optional[int] = None,
        uppercase_table: bool = True,
        **values) -> str:
    """ generate sql command from config file

    Uses config file and kwargs to generate SQL query statements. The named query statements
    are given as a dict whose keys are the names and the values are configs as discussed in
    doc-string for QueryConstructor.from_config above.  Additionally, default values for the
    initializer can be added to be used in all "named queries".


    Additionally, pass a <table> to construct simple quieres (where and limit only)
    on a single table.

    Config Description:

    queries-config files have the following parts

    ```yaml
    project: |
        [optional] (str) gcp project-name used to generate default table_prefix
        if table_prefix absent. ie table_prefix = project.dataset

    dataset: |
        [optional] (str) gcp dataset-name used to generate default table_prefix
        if table_prefix absent. ie table_prefix = project.dataset

    defaults: |
        [optional] (dict) default init values for all named queries. may contain
        all arguments to QueryConstructor(...) except `table`.
        namely: `table_prefix`, `how`, `on`, and `using`.

        notes:
            - if table_prefix is absent/empty and `project` and/or `dataset` exist a
              table_prefix = project.dataset will be added.
            - `init` dicts in `queries` dicts below will override the defaults

    queries: |
        (dict) key-value pairs of query-name, query-config. the key value pairs are

        key: (str) name of query
        value: |
            (dict | list[dict]) dicts of args/kwargs to QueryConstructor __init__
            method (key = `init`) as well as each of QueryConstructor main public methods.
            Namely: `select`, `join`, `where`, `append`, and `limit`.

            if value is dict the dict args will be passed to named method
            if value is list[dict] for each element of value the dict args
            will be passed to named method
    ```

    Example:

    ```yaml

    ...

    queries:
      raw_landsat:
        init:
            table: SAMPLE_POINTS
            using: sample_id
        join:
            table: LANDSAT_RAW_MASKED
      raw_landsat_between_2020_and_2020:
        init:
            table: SAMPLE_POINTS
        join:
            table: LANDSAT_RAW_MASKED
        where:
            - year: 2010
              year_op: '>='
            - year: 2020
              year_op: <
        limit: 23
      scym_raw_landsat:
        init:
            table: SAMPLE_POINTS
        join:
            - table: SCYM_YIELD
            - table: LANDSAT_RAW_MASKED
              using: sample_id, year
    ```

    Notes:

        - The gcp project and dataset are set using the `project`/`dataset` values. These will
          be used to prepend table/join_table names if the table/join_table names do not contain '.'
        - The `defaults` dict can set default `how`, `on`, and `using` passed to the
          QueryConstructor initializer
        - `queries` is a dictionary with all the named queries. We start my adding creating a
          select statement 'SELECT {select} FROM {table}', where "{}" indicate the value
          subtracted from the named-query dict or the defaults dict. Then we sequentially loop
          over the join list using the {table} and {join} values.

    Examples:

    for

    ```yaml
    project: dse-regenag
    dataset: BiomassTrends
    defaults:
        using: sample_id
    queries:
        raw_landsat:
            init:
                table: SAMPLE_POINTS
            join:
                table: LANDSAT_RAW_MASKED
                using: sample_id, year
    ```

    `named_sql('raw_landsat')` will output

    ```sql
    SELECT * FROM `dse-regenag.BiomassTrends.SAMPLE_POINTS`
    LEFT JOIN `dse-regenag.BiomassTrends.LANDSAT_RAW_MASKED`
    USING (sample_id, year)
    ```

    We can also add a `where` key:

    ``` yaml
      scym_raw_for_2012:
        table: SAMPLE_POINTS
        join:
          - table: SCYM_YIELD
          - table: LANDSAT_RAW_MASKED
            using: sample_id, year
        where:
            year: 2012
    ```

    now `named_sql('scym_raw_for_2012')` will output

    ```sql
    SELECT * FROM `dse-regenag.BiomassTrends.SAMPLE_POINTS`
    LEFT JOIN `dse-regenag.BiomassTrends.LANDSAT_RAW_MASKED`
    USING (sample_id, year)
    WHERE `dse-regenag.BiomassTrends.SAMPLE_POINTS`.year = 2012
    ```

    That said, hard coding the where might not be what is desired you can
    also do this `named_sql('raw_landsat', year=2012)` to achieve the same
    result.

    Args:

        name (Optional[str]=None): name of preconfigured config file
        table (Optional[str]=None):
            (required if name is None) table-name: queries a
            single table with optional `WHERE` clause added through
            `values` kwargs.
        select (Optional[str] = None):
            a simple select string ie 'col1, col2, col3 as c3'
        config (Union[str,dict]=c.DEFAULT_QUERY_CONFIG):
            configuration dictionary containg sql-config with key <name>
            if (str):
                if re.search(r'(yaml|yml)$', <config>) loads yaml file with at <path config>
                else loads yaml at '<project-root>/config/named_queries/<config>.yaml'
        limit (int=None):
            if exits add "LIMIT <limit>" to end of SQL call
        uppercase_table (bool = True):
            if true apply `.upper()` to <table>. Note only used if <table>
            is non-null.
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
    elif not config:
        config = {}
    assert isinstance(config, dict)
    if table:
        if uppercase_table:
            table = table.upper()
        config['init'] = config.get('defaults', {})
        config['init']['table'] = table
        config = process_named_query_config(config, name)
    elif name:
        config = process_named_query_config(config, name)
    qc = QueryConstructor.from_config(config)
    if select:
        qc.select(select)
    if values:
        qc.where(**values)
    if limit:
        qc.limit(limit)
    return qc.sql()


def table_names(
        project: str = c.GCP_PROJECT,
        database: str = c.DATASET_NAME,
        select: str = 'table_name',
        run_query: bool = True,
        to_list: bool = True,
        to_dataframe: bool = True) -> Union[str, bq.QueryJob, pd.DataFrame]:
    sql = f"select {select} from `{project}.{database}.INFORMATION_SCHEMA.TABLES`"
    if run_query:
        result = run(sql=sql, to_dataframe=to_dataframe or to_list)
        if to_list:
            return result.table_name.tolist()
        else:
            return result
    else:
        return sql


def column_names(
        table: str,
        project: str = c.GCP_PROJECT,
        database: str = c.DATASET_NAME,
        select: str = 'column_name',
        run_query: bool = True,
        to_list: bool = True,
        to_dataframe: bool = True) -> Union[str, bq.QueryJob, pd.DataFrame]:
    sql = f"select {select} from `{project}.{database}.INFORMATION_SCHEMA.COLUMNS`"
    if table.upper() not in ['*', 'ALL']:
        sql += f' WHERE table_name = "{table}"'
    if run_query:
        result = run(sql=sql, to_dataframe=to_dataframe or to_list)
        if to_list:
            return result.column_name.tolist()
        else:
            return result
    else:
        return sql


def run(
        name: Optional[str] = None,
        table: Optional[str] = None,
        select: Optional[str] = None,
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
    if sql and limit:
        sql += f' LIMIT {limit}'
    elif name or table:
        sql = named_sql(
            name=name,
            table=table,
            select=select,
            config=config,
            limit=limit,
            **values)
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
def _safe_prepend_keys(prefix, value, keys=TABLE_KEYS):
    if isinstance(value, dict):
        for key in keys:
            v = value.get(key)
            if isinstance(v, str) and ('.' not in v):
                value[key] = f'{prefix}.{v}'
    return value
