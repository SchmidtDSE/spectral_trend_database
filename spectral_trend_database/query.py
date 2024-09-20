""" utility methods

License:
    BSD, see LICENSE.md
"""
from typing import Optional, Union, Any
import re
import pandas as pd
from google.cloud import bigquery as bq
from spectral_trend_database.config import config as c
from spectral_trend_database import utils


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
            values for where clause (see usage above)

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
    if table:
        table = table.upper()
        cfig = config.get('defaults', {})
        cfig['table'] = table
        cfig['where'] = []
        for k, v in values.items():
            where_config = {}
            where_config['key'] = k
            where_config['table'] = table
            where_config['value'] = v
            cfig['where'].append(where_config)
    else:
        err = (
            'ndvi_trends.query.named_sql: '
            'either <name> or <table> must be non-null'
        )
        raise ValueError(err)
    sql = f"SELECT {cfig['select']} FROM `{dataset}{cfig['table']}`"
    for join in cfig.get('join', []):
        jcfig = {**cfig, **join}
        sql += f" {jcfig['how']} JOIN `{dataset}{jcfig['table']}`"
        sql += f" USING ({jcfig['using']})"
    for i, where in enumerate(cfig.get('where', [])):
        table = where['table']
        key = where['key']
        try:
            value = where['value']
        except:
            value = values[key]
        if i:
            where_sql = ' AND'
        else:
            where_sql = ' WHERE'
        sql += f'{where_sql} `{dataset}{table}`.{key} = {value}'
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
        **values) -> pd.DataFrame:
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
    return client.query(sql).to_dataframe()
