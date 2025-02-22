import importlib.util
import sys
from copy import deepcopy
from spectral_trend_database import utils


#
# HELPERS
#
def import_module(path):
    spec = importlib.util.spec_from_file_location("module.name", path)
    module = importlib.util.module_from_spec(spec)
    sys.modules["module.name"] = module
    spec.loader.exec_module(module)
    return module


#
# MAIN
#
class JobRunner(object):
    """
    TOOD: FIX DOC STRING

    1. jr = JobRunner(config): here config must have a jobs-list
    and perhaps a shared-config. if shared-config then that
    becomes the base config

    2. jr.run(..., run_config): this is from user passed BLAH=123
    and overrides everything else

    3. jr.run_job(..., job_config): these are per job configs contained
    in a job from the jobs-list mentioned in (1)

    order of configs (each updates last)

    a. config = shared_config or {}
    b. if config has key which matches the job name
       pop that out and add update the config with that
    c. update with "job config" from a job in the per job configs contained
       in a job from the jobs-list mentioned in (1)
    d. update with any user-passed config BLAH=123 in the cli
    """
    def __init__(self, config):
        _config = utils.process_config(config)
        self.config = utils.process_config(_config.get('shared_config', {}))
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
        TODO: FIX DOC STRINGS
        each job must have name + optional file and/or doc string
        """
        print()
        print(f'{name} [start]:', self.timer.now())
        job_config = self._process_job_config(name, config)
        if file:
            path =  utils.full_path(file, ext='py')
        else:
            # raise NotImplementedError('TODO: stdb jobs if no file')
            print('TODO: stdb jobs if no file')
            return
        job_interface = import_module(path)
        job_interface.run(job_config)
        print(f'{name} [complete]:', self.timer.now())
        print()

    #
    # INTERNAL
    #
    def _select_jobs(self, name, start_index, end_index):
        jobs = self.jobs
        if name:
            job = next((j for j in jobs if j.get('name') == name), False)
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
            jobs = jobs[start_index:end_index]
        return jobs

    def _process_job_config(self, name, job_config):
        config = deepcopy(self.config)
        config.update(config.get(name, {}))
        config.update(utils.process_config(job_config))
        config.update(deepcopy(self.run_config))
        return config
