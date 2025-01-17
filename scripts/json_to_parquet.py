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
from spectral_trend_database import runner


#
# CONFIG
#
GCS_DB_JSON = f'{c.GCS_BUCKET}/{c.GCS_ROOT_FOLDER}'
GCS_DB_PARQ = f'{c.GCS_BUCKET}/{c.GCS_PARQUET_FOLDER}'
DRY_RUN = c.DRY_RUN


#
# METHODS
#
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
        files = gcp.gcs_list(folder)
        gcs_dest = f"{re.sub(f'^{GCS_DB_JSON}', GCS_DB_PARQ, folder)}.parquet"
        local_dest = paths.local(gcs_dest)
        if DRY_RUN:
            print('- (dry_run) local:', local_dest)
            print('- (dry_run) gcs:', gcs_dest)
        else:
            Path(local_dest).parent.mkdir(parents=True, exist_ok=True)
            df = pd.concat([pd.read_json(f, lines=True) for f in files])
            print('- shape:', df.shape)
            if 'year' in df.columns:
                df.to_parquet(local_dest, partition_cols=['year'])
                for p in Path(local_dest).glob('*/*'):
                    _dest = f'{gcs_dest}/{p.parent.name}/{p.name}'
                    runner.save_to_gcp(
                        src=p,
                        gcs_dest=_dest,
                        table_name=None,
                        dataset_name=None,
                        remove_src=True,
                        dry_run=DRY_RUN)
            else:
                df.to_parquet(local_dest)
                runner.save_to_gcp(
                    src=local_dest,
                    gcs_dest=gcs_dest,
                    table_name=None,
                    dataset_name=None,
                    remove_src=True,
                    dry_run=DRY_RUN)


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
pprint(FOLDERS)
for i, f in enumerate(FOLDERS):
    print('\n' * 2)
    print('--' * 50)
    print(f'[{i}]', f)
    print('--' * 50)
    process_folder(f)
