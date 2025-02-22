""" CONVERT JSON FILES TO PARQUET

authors:
    - name: Brookie Guzder-Williams

affiliations:
    - University of California Berkeley,
      The Eric and Wendy Schmidt Center for Data Science & Environment

note:

    because of year partitioning you must be working with a clean local
    directory. future versions will account for this.

description:

    for each (sub)folder in gcp GCS_DB_JSON convert json file to parquet and
    save to gcp GCS_DB_PARQ

runtime: ~ XXX minutes

License:
    BSD, see LICENSE.md
"""
import re
from pathlib import Path
from pprint import pprint
import pandas as pd
from spectral_trend_database.config import config as c
from spectral_trend_database import gcp
from spectral_trend_database import paths
from spectral_trend_database import interface


#
# CONFIG
#
GCS_DB_JSON = f'{c.GCS_BUCKET}/{c.GCS_ROOT_FOLDER}'
GCS_DB_PARQ = f'{c.GCS_BUCKET}/{c.GCS_PARQUET_FOLDER}'
START_INDEX = 0
DRY_RUN = c.DRY_RUN


#
# CONSTANTS
#
RGX_YYYYJ = r'-(19|20)([89]|[0-3])[0-9].json$'


#
# METHODS
#
def save_as_parquet(df, local_dest, gcs_dest, partitioned=False):
    if partitioned:
        print('- shape[partitioned]:', df.shape)
        df.to_parquet(local_dest, partition_cols=['year'])
        for path in Path(local_dest).glob('*/*'):
            _dest = f'{gcs_dest}/{path.parent.name}/{path.name}'
            interface.save_to_gcp(
                src=path,
                gcs_dest=_dest,
                table_name=None,
                dataset_name=None,
                remove_src=True,
                dry_run=DRY_RUN)
    else:
        print('- shape:', df.shape)
        df.to_parquet(local_dest)
        interface.save_to_gcp(
            src=local_dest,
            gcs_dest=gcs_dest,
            table_name=None,
            dataset_name=None,
            remove_src=True,
            dry_run=DRY_RUN)


def process_folder(
        folder,
        process_subfolders=True):
    subfolders = gcp.gcs_list_folders(folder)
    if subfolders:
        if process_subfolders:
            for f in subfolders:
                process_folder(f)
        else:
            print('[WARNING] subfolders not being processed:', subfolders)
    else:
        gcs_dest = f"{re.sub(f'^{GCS_DB_JSON}', GCS_DB_PARQ, folder)}.parquet"
        local_dest = paths.local(gcs_dest)
        files = gcp.gcs_list(folder)
        nb_files = len(files)
        if DRY_RUN:
            print(f'- (dry_run) local [{nb_files}]:', local_dest)
            print(f'- (dry_run) gcs [{nb_files}]:', gcs_dest)
        else:
            if nb_files == 1:
                df = pd.read_json(files[0], lines=True)
                Path(local_dest).parent.mkdir(parents=True, exist_ok=True)
                if 'year' in df.columns:
                    save_as_parquet(df, local_dest, gcs_dest, partitioned=True)
                else:
                    save_as_parquet(df, local_dest, gcs_dest, partitioned=False)
            elif are_yyyy_partitioned(files):
                Path(local_dest).parent.mkdir(parents=True, exist_ok=True)
                for file in files:
                    df = pd.read_json(file, lines=True)
                    save_as_parquet(df, local_dest, gcs_dest, partitioned=True)
            else:
                err = (
                    'if non-partitioned only 1 file may exist, '
                    f'found {nb_files}:\n'
                    f'{files}')
                raise ValueError(err)


def are_yyyy_partitioned(files):
    nb_files = len(files)
    searches = [re.search(RGX_YYYYJ, f) is not None for f in files]
    if nb_files == 1:
        return searches[0]
    else:
        yyyy_searches = [True for v in searches if v]
        nb_yyyy = len(yyyy_searches)
        if nb_yyyy:
            if nb_yyyy == nb_files:
                return True
            else:
                raise ValueError('recieved mixed yyyy/not-yyyy files')
        else:
            return False


#
# RUN
#
print()
print('saving json to parquet:')
print()
print(f'- src: gs://{GCS_DB_JSON}')
print(f'- dest: gs://{GCS_DB_PARQ}')
FOLDERS = gcp.gcs_list_folders(GCS_DB_JSON)
FOLDERS = sorted(FOLDERS)
print('- source directories:')
pprint(FOLDERS[START_INDEX:])
for i, f in enumerate(FOLDERS[START_INDEX:]):
    print('\n' * 2)
    print('--' * 50)
    print(f'[{i + START_INDEX}]', f)
    print('--' * 50)
    process_folder(f)
