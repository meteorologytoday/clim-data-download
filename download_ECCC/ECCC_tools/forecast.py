import pandas as pd

# The bounds are included
model_version_date_bounds = dict(
    GEPS5 = [ pd.Timestamp("2018-09-27"), pd.Timestamp("2019-06-27"), ],
    GEPS6 = [ pd.Timestamp("2019-07-24"), pd.Timestamp("2021-11-25"), ],
    GEPS7 = [ pd.Timestamp("2021-12-02"), pd.Timestamp("2024-06-12"), ],
    GEPS8 = [ pd.Timestamp("2024-06-13"), pd.Timestamp("2028-12-31"), ],
)

def checkIfModelVersionDateIsValid(model_version, dt):
    
    rng = model_version_date_bounds[model_version]

    if dt >= rng[0] and dt < rng[1]:
        return True

    else:
        return False
