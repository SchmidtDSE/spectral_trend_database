from pathlib import Path
import re
import yaml
from copy import deepcopy
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
_NOT_FOUND: str = '_NOT_FOUND'
DEFAULT_CONFIG = 'stdb.config.yaml'


#----------------------------------------------------
#
# DEV: TO BE MOVED TO ITS OWN MODULE
#
import importlib.util
import sys

def import_module(path):
    spec = importlib.util.spec_from_file_location("module.name", path)
    module = importlib.util.module_from_spec(spec)
    sys.modules["module.name"] = module
    spec.loader.exec_module(module)
    return module

class JobRunner(object):

    def __init__(self, config):
        print(1, config)
        _config = utils.process_config(config)
        self.config = utils.process_config(_config.get('shared_config', {}))
        print(2, self.config)
        self.jobs = _config['jobs']
        self.timer = utils.Timer()


    def run(self, name=None, start_index=None, end_index=None, run_config={}):
        self.run_config = utils.process_config(run_config)
        jobs = self._select_jobs(name, start_index, end_index)
        print('- start:', self.timer.start())
        print('- run jobs:', [j['name'] for j in jobs])
        print('-' * 100)
        for job in jobs:
            self.run_job(**job)
        print('-' * 100)
        end_time = self.timer.stop()
        duration = self.timer.delta()
        print(f'- complete [{duration}]: {end_time}')

    def run_job(self, name, file=None, config={}):
        """
        -
        """
        print()
        print(name, self.timer.now())
        print(111,config)
        config = self._process_job_config(name, config)
        print(222, config)
        print('-----------')
        if file:
            path =  utils.full_path(file, ext='py')
        else:
            # raise NotImplementedError('TODO: stdb jobs if no file')
            print('TODO: stdb jobs if no file')
            return
        job_interface = import_module(path)
        job_interface.run(config)
        print()


    #
    # INTERNAL
    #
    def _select_jobs(self, name, start_index, end_index):
        if name:
            job = next((j for j in self.jobs if j.get('name') == name), False)
            if not job:
                err = (
                    'spectral_trend_database.JobRunner._select_jobs: '
                    f'job "{name}" not found in {jobs}'
                )
                raise ValueError(err)
            jobs = [job]
        elif start_index:
            if end_index:
                end_index = end_index + 1
            else:
                end_index = None
            jobs = self.jobs[start_index:end_index]
        return jobs


    def _process_job_config(self, name, job_config):
        print(1111, name, job_config)
        config = deepcopy(self.config)
        print(2222, config)
        config.update(config.get(name, {}))
        print(3333, config)
        config.update(utils.process_config(job_config))
        print(4444, config)
        config.update(deepcopy(self.run_config))
        print(5555, config)
        return config

#
#
#----------------------------------------------------


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
    name, run_config = _pocess_name_and_context(name, ctx.args)
    _check_argument_exclusions(
        name=name,
        index=index,
        index_range=index_range,
        run_all=run_all)
    name, start_index, end_index = _process_job_options(
        name=name,
        index=index,
        index_range=index_range,
        run_all=run_all)
    runner = JobRunner(config)
    runner.run(
        name=name,
        start_index=start_index,
        end_index=end_index,
        run_config=run_config)



#
# HELPERS
#
def _pocess_name_and_context(name, ctx_args):
    args=[]
    config={}
    if name and ('=' in name):
        k, v = name.split('=')
        name = None
        config[k] = v
    for a in ctx_args:
        if re.search('=',a):
            k,v=a.split('=')
            config[k]=v
        else:
            args.append(a)
    if args:
        err = (
            'spectral_trend_database.cli._ctx_args: '
            f'additional command line arguments passed [{args}]'
        )
        raise ValueError(err)
    return name, config


def _process_job_options(name, index, index_range, run_all):
    if name:
        return name, None, None
    elif index:
        return None, index, None
    elif index_range:
        start, end = [int(v.strip())-1 for v in index_range.split(',')]
        return None, start, end
    elif not run_all:
        err = (
            'spectral_trend_database.cli._process_job_options: '
            'one of [name, index, index_range, run_all] '
            'be passed to job'
        )
        raise ValueError(err)
    else:
        return None, None, None


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


#
# MAIN
#
cli.add_command(job)
