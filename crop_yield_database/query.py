""" utility methods

License:
    BSD, see LICENSE.md
"""
from typing import Optional, Union
import re
import pandas as pd
from google.cloud import bigquery as bq
import crop_yield_database.constants as c
import crop_yield_database.utils as utils


#
# CONSTANTS
#
QUERY_REQUIRED = '__query_required'


#
# METHODS
#
def named_sql(
        name: str,
        config: Union[dict, str] = c.DEFAULT_QUERY_CONFIG,
        limit: Optional[int] = None,
        **values) -> str:
    """ generate sql command from config file

    Usage:

    Args:
        name (str): name of preconfigured config file
        config (Union[str,dict]=c.DEFAULT_QUERY_CONFIG):
            configuration dictionary containg sql-config with key <name>
            if (str) loads yaml file with at '<project-root>/config/<config>.yaml'
        limit (int=None):
            if exits add "LIMIT <limit>" to end of SQL call
        **values:
            values for where clause (see usage above)

    Returns:
        (str) sql command
    """
    if isinstance(config, str):
        config = utils.read_yaml(f'{c.NAMED_QUERY_DIR}/{config}.yaml')
    project = config.get('project')
    dataset = config.get('dataset')
    if dataset:
        if project:
            dataset = f'{project}.{dataset}'
        dataset += '.'
    else:
        dataset = ''
    cfig = {**config.get('defaults', {}), **config['queries'][name]}
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
        config: Union[dict, str] = c.DEFAULT_QUERY_CONFIG,
        limit: Optional[int] = None,
        sql: str = QUERY_REQUIRED,
        print_sql: bool = False,
        project: Optional[str] = None,
        client: Optional[bq.Client] = None,
        **values) -> pd.DataFrame:
    """ queries bigquery

    Usage: TODO

    Args:
        name (str): name of preconfigured config file
        config (Union[str,dict]=c.DEFAULT_QUERY_CONFIG):
            configuration dictionary containg sql-config with key <name>
            if (str) loads yaml file with at '<project-root>/config/<config>.yaml'
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
    if name:
        sql = named_sql(name=name, config=config, limit=limit, **values)
    if sql == QUERY_REQUIRED:
        raise ValueError('crop_yield_database.query.run: name query or explict sql required')
    if print_sql:
        print(f'crop_yield_database.query.run: {sql}')
    return client.query(sql).to_dataframe()
