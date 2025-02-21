from pathlib import Path
import re
import yaml
from pprint import pprint
import click
from spectral_trend_database import utils
from spectral_trend_database.config import ConfigHandler
"""
```yaml
# stdb_config.py
root_dir: /users/repo/root/dir
config_folder: if/exists/append/config/paths/below
user_config: path/to/user_config.yaml
indices_config: path/to/spectral_index_config.yaml
job_folder: jobs/
jobs:
    - name: my_job_step1
      file: my_job_step1.py
      config:
        var1: 1
        var2: 123
    # if config is string import from file
    - name: my_job_step2
      file: my_job_step2.py
      config: job2.yaml
    # no config is required
    - name: config_not_required_step3
      file: my_job_step3.py
    # jobs without `file` run pre-defined jobs from stdb
    - name: step-0.samples_and_qdann_yield
```





"""
#
# CONSTANTS
#
DRY_RUN: bool = False
CTX_SETTINGS: dict = dict(allow_extra_args=True)
MUTUALLY_EXCLUSIVE_ARGS: list[str] = [
    'name',
    'index',
    'index_range',
    'run_all' ]
FULL_PATH_PREFIXES: list[str] = ['~', '/']
YAML_REGEX: list[str] = r'\.(yml|yaml)$'
YAML_EXT: str = 'yaml'
FALSEY: list[str] = ['None', 'none', 'null', 'false', 'False', '0']
_NOT_FOUND: str = '_NOT_FOUND'
DEFAULT_CONFIG = 'stdb.config.yaml'


class JobRunner(object):

    def __init__(self, jobs):
        self.jobs = jobs
        self.timer = utils.Timer()

    def run(self):
        print('- start:', self.timer.start())
        print('- run jobs:', [j['name'] for j in self.jobs])
        print('-' * 100)
        for job in self.jobs:
            self.run_job(**job)
        print('-' * 100)
        end_time = self.timer.stop()
        duration = self.timer.delta()
        print(f'- complete [{duration}]: {end_time}')

    def run_job(self, name, file=None, config=None):
        print()
        print(name, self.timer.now())
        print(file)
        pprint(config)
        print()

#
# CLI INTERFACE
#
@click.group
@click.pass_context
def cli(ctx):
    ctx.obj = {}


@cli.command(name='job', help='job help text', context_settings=CTX_SETTINGS)
@click.argument('name', type=str, required=False)
@click.option('-i', '--index', type=int, required=False)
@click.option('-r', '--range', 'index_range',type=str, required=False)
@click.option('-a', '--all', 'run_all', is_flag=True, required=False)
@click.option('-c', '--config', required=False, default=DEFAULT_CONFIG)
@click.option('-l', '--limit', type=int, required=False)
@click.option('--dry_run', type=bool, required=False, default=DRY_RUN)
@click.pass_context
def job(
        ctx,
        name,
        index,
        index_range,
        run_all,
        config,
        limit=None,
        dry_run=DRY_RUN):
    _check_argument_exclusions(
        name=name,
        index=index,
        index_range=index_range,
        run_all=run_all)
    kwargs = _ctx_args(ctx.args)
    config = _process_config(config)
    jobs = _get_jobs(config, name, index, index_range, run_all)
    runner = JobRunner(jobs)
    runner.run()



#
# HELPERS
#
def _ctx_args(ctx_args):
    args=[]
    kwargs={}
    for a in ctx_args:
        if re.search('=',a):
            k,v=a.split('=')
            kwargs[k]=v
        else:
            args.append(a)
    if args:
        err = (
            'spectral_trend_database.cli._ctx_args: '
            f'additional command line arguments passed [{args}]'
        )
        raise ValueError(err)
    return kwargs

def _non_trival_arg(value):
    trivial = (
        (value == _NOT_FOUND) or
        (value is None) or
        (value is False))
    return not trivial

def _check_argument_exclusions(
        mutually_exclusive=MUTUALLY_EXCLUSIVE_ARGS,
        **kwargs):
    arg_names = [(n, kwargs.get(n, _NOT_FOUND)) for n in mutually_exclusive]
    arg_names = [n for n, v in arg_names if _non_trival_arg(v)]
    if len(arg_names) > 1:
        err = (
            'spectral_trend_database.cli._check_argument_exclusions: '
            'multiple mutually exclusive command line arguments passed '
            f'[{arg_names}]'
        )
        raise ValueError(err)

def _process_path(path):
    if not re.search(YAML_REGEX, path):
        path = f'{path}.{YAML_EXT}'
    if path[0] not in FULL_PATH_PREFIXES:
        path = f'{Path.cwd()}/{path}'
    return path

def _get_jobs(config, name, index, index_range, run_all):
    jobs = config['jobs']
    if name:
        jobs = [next(j for j in jobs if j.get(name))]
    elif index:
        jobs = jobs[index:index+1]
    elif index_range:
        i, j = [int(v.strip())-1 for v in index_range.split(',')]
        jobs = jobs[i:j+1]
    elif not run_all:
        err = (
            'spectral_trend_database.cli._get_jobs: '
            'must pass one of [name, index, index_range, run_all]'
        )
        raise ValueError(err)
    return jobs

def _process_config(config):
    if config in FALSEY:
        config = {}
    else:
        path = _process_path(config)
        if Path(path).is_file():
            config = utils.read_yaml(path)
        else:
            err = (
                'spectral_trend_database.cli._process_config: '
                'stdb config file does not exist '
                f'[{path}]'
            )
            raise ValueError(err)
    return config


#
# MAIN
#
cli.add_command(job)
