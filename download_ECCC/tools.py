import pandas as pd

# The bounds are included
model_version_date_bounds = dict(
    GEPS5 = [ pd.Timestamp("2018-09-27"), pd.Timestamp("2019-06-27"), ],
    GEPS6 = [ pd.Timestamp("2019-07-24"), pd.Timestamp("2021-11-25"), ],
)




# Load the data
valid_model_version_dates = dict(GEPS5=[], GEPS6=[])
with open("model_version_dates.txt", "r") as f:
    for s in f:
        if s != "":
            ts = pd.Timestamp(s)
            for model_version in ["GEPS5", "GEPS6"]:
                bnds = model_version_date_bounds[model_version]
                if ts >= bnds[0] and ts <= bnds[1]:
                    valid_model_version_dates[model_version].append(ts)

def printValidModelVersionDates():
     for model_version in ["GEPS5", "GEPS6"]:
        for i, model_version_date in enumerate(valid_model_version_dates[model_version]):
            print("[%d] %s " % (i, model_version_date.strftime("%Y-%m-%d")))




"""
    This function receives model_version GEPS5 or GEPS6
    and reforecast date.

    The function returns model_version_date
"""
def model_version_reforecast_date_mapping(model_version, reforecast_date):
    
    # IMPORTANT:
    # Notice that there is no two reforecast_date of the same model_version
    # map to two model_version_date
    # The model_version_date is unique with the same model_version
    # Therefore, a specific month-day will map to a specific model_version_date
    # for either GEPS5 or GEPS6

    if model_version == "GEPS5":
        pass
        #if reforecast_date in  
        
        
if __name__ == "__main__":   
    printValidModelVersionDates()


