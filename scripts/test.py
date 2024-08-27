import numpy as np
import xarray as xr
from pprint import pprint
from IPython.display import display
import spectral_trend_database.constants as c
from spectral_trend_database import utils
import spectral_trend_database.query as query
import spectral_trend_database.gcp as gcp
import spectral_trend_database.spectral as spectral

scfig = spectral.index_config()
pprint(scfig)


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
