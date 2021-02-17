import pandas as pd

def zillow(file_dict, file_location):
    """
    Input
    -----
    file_dict: dictionary of files with key as filename and value as link to data
    file_location: destination folder for data

    Output
    ------
    file: text file to Updates folder
    """

    for key, value in file_dict.items():

        # read data
        df = pd.read_csv(value)

        # fill state code fips to 2 places
        df["StateCodeFIPS"] = df["StateCodeFIPS"].astype(str)
        df["StateCodeFIPS"] = df["StateCodeFIPS"].str.zfill(2)

        # fill municipal code fips to 3 places
        df["MunicipalCodeFIPS"] = df["MunicipalCodeFIPS"].astype(str)
        df["MunicipalCodeFIPS"] = df["MunicipalCodeFIPS"].str.zfill(3)

        df["Region Code"] = df["StateCodeFIPS"] + df["MunicipalCodeFIPS"]

        # drop junk
        cols_to_drop = [
            "RegionID",
            "SizeRank",
            "RegionType",
            "State",
            "Metro",
            "StateCodeFIPS",
            "MunicipalCodeFIPS",
        ]
        df = df.drop(cols_to_drop, axis=1)

        # create measure column
        df["Measure Name"] = key

        # split region name
        df["RegionName"] = df["RegionName"].astype(str)
        df = df.rename(columns={"RegionName": "Region Name"})
        df["Region Name"] = df["Region Name"] + ", " + df["StateName"]
        df.drop("StateName", axis=1, inplace=True)

        # Melt df
        df = df.melt(
            id_vars=["Region Code", "Region Name", "Measure Name"],
            value_name="Estimated Value",
            var_name="Date",
        )
        df.set_index("Region Code", inplace=True)

        df.to_csv("./" + file_location + "/Data/" + key + ".txt", sep="\t")
