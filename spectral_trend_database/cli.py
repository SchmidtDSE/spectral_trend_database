import click
import re

"""
```bash
# stdb_config should live in directory
# or could be passed with flag `stdb job -c path/to/config.yaml -i 1`
# should the job name/indices be flags or just args
# run named job
pixi run stdb job -n my_job_step2

# run the second job
pixi run stdb job -i 1

# run jobs 1, 2 and 3
pixi run stdb job -r 1,3

# run all jobs in order
pixi run stdb job --a

# we should probably create a "job" task
pixi run job -r 1,3
pixi run job --a
```

"""
#
# CONSTANTS
#
DRY_RUN: bool = False
CTX_SETTINGS: dict = dict(allow_extra_args=True)
MUTUALLY_EXCLUSIVE_ARGS: dict[str] = [
    'name',
    'index',
    'index_range',
    'run_all' ]
_NOT_FOUND: str = '_NOT_FOUND'


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
@click.option('-c', '--config', type=int, required=False)
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
    print('STDB JOB', ctx, name)
    _check_argument_exclusions(
        name=name,
        index=index,
        index_range=index_range,
        run_all=run_all)
    kwargs = _ctx_args(ctx)
    print(kwargs)
    print(index, index_range, run_all, dry_run)


#
# HELPERS
#
def _ctx_args(ctx):
    args=[]
    kwargs={}
    for a in ctx.args:
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


#
# MAIN
#
cli.add_command(job)
