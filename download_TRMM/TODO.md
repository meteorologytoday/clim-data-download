


- Use python package `tempfile` to do download.
- Use grouop `xr.open_dataset(filename, engine="h5netcdf", group="/Grid/Intermediate")` to access.

Use the following code

```
import xarray as xr
import earthaccess
from io import BytesIO

earthaccess.login()

session = earthaccess.get_requests_https_session()

r = session.get("Link_to_file")
ds = xr.open_dataset(BytesIO(r.content), engine="h5netcdf", group="/Grid/Intermediate")


```
