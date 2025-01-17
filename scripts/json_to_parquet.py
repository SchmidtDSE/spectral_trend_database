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
GCS_DB_JSON = 'agriculture_monitoring/spectral_trend_database/v1/qdann'
GCS_DB_PARQ = 'agriculture_monitoring/spectral_trend_database/v1/parquet/qdann'
DRY_RUN = c.DRY_RUN


#
# METHODS
#
def process_folder(folder, process_subfolders=True):
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
            df = pd.concat([pd.read_json(f, lines=True) for f in files])
            print('- shape:', df.shape)
            if True or 'year' in df.columns:
                config = dict(partition_cols=['year'])
            else:
                config = {}
            Path(local_dest).parent.mkdir(parents=True, exist_ok=True)
            print('- local:', local_dest)
            df.to_parquet(local_dest, **config)
            if config:
                print('- partitioned by year')
                for p in Path(local_dest).glob('*/*'):
                    print('\t- gcs:', gcs_dest)
                    runner.save_to_gcp(
                        src=p,
                        gcs_dest=gcs_dest,
                        table_name=None,
                        dataset_name=None,
                        remove_src=True,
                        dry_run=DRY_RUN)
            else:
                print('- gcs:', gcs_dest)
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
print('saving json to parquet:')
print(f'- src: gs://{GCS_DB_JSON}')
print(f'- dest: gs://{GCS_DB_PARQ}')
FOLDERS = gcp.gcs_list_folders(GCS_DB_JSON)
FOLDERS = sorted(FOLDERS)
print('- source directories:')
pprint(FOLDERS)
for f in FOLDERS:
    print('\n' * 2)
    print('--' * 50)
    print(f)
    print('--' * 50)
    process_folder(f)
