import pandas as pd

# The bounds are included


infos = dict(
    ecmf = dict(
        CY48R1 = dict(
            model_version_date_bounds = ( pd.Timestamp("2023-06-27"), pd.Timestamp("2024-11-11"), ),
            ensemble_members = 100,
        ),
 
        CY49R1 = dict(
            model_version_date_bounds = ( pd.Timestamp("2024-11-12"), pd.Timestamp("2028-12-31"), ),
            ensemble_members = 100,
        ),
        
    ),

    cwao = dict(
        GEPS5 = dict(
            model_version_date_bounds = ( pd.Timestamp("2018-09-27"), pd.Timestamp("2019-06-27"), ),
        ),
        
        GEPS6 = dict(
            model_version_date_bounds = ( pd.Timestamp("2019-07-24"), pd.Timestamp("2021-11-25"), ),
        ),

        GEPS7 = dict(
            model_version_date_bounds = ( pd.Timestamp("2021-12-02"), pd.Timestamp("2024-06-12"), ),
        ),

        GEPS8 = dict(
            model_version_date_bounds = ( pd.Timestamp("2024-06-13"), pd.Timestamp("2028-12-31"), ),
            ensemble_members = 20,
        ),

    ),
)


def checkIfModelVersionDateIsValid(origin, model_version, dt):
    
    info = infos[origin][model_version]
    rng = info["model_version_date_bounds"]

    result = True
    if dt >= rng[0] and dt < rng[1]:
        result = True

    else:
        result = False


    if origin == "cwao":
        
        days_of_week = []

        if model_version == "GEPS8":
            days_of_week = [0, 3] # Mon and Thu
        elif model_version in ["GEPS5", "GEPS6", "GEPS7"]:
            days_of_week = [3, ] # Thu only
        
        if dt.weekday() not in days_of_week:
            result = False

    elif origin == "ecmf":
 
        # https://confluence.ecmwf.int/display/S2S/ECMWF+Model

        if model_version == "CY49R1":
            if dt.day % 2 != 1:
               result = False 
        elif model_version == "CY48R1":
            days_of_week = [0, 3] # Mon and Thu
        
            if dt.weekday() not in days_of_week:
                result = False

    print("result: ", result)
    return result
