import numpy as np
import xarray as xr
from IPython.display import display
import crop_yield_database.constants as c
from crop_yield_database import utils
import crop_yield_database.query as query
import crop_yield_database.gcp as gcp


LSAT_BANDS = [
    'blue',
    'green',
    'red',
    'nir',
    'swir1',
    'swir2'
]


df = query.run('scym_raw', print_sql=True, year=2018, limit=2)
print(df.shape)


row = df.sample().iloc[0]
display(row)


row['date'] = row.date.astype('datetime64[ns]')


out = utils.pandas_to_xr(row, 'date', LSAT_BANDS)
display(out)
