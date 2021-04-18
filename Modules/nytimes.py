import pandas as pd


def covid(file_dict, file_location):
    """
    Input
    -----
    file_dict: dictionary of files with key as filename and value as link to data
    file_location: main destination folder for data files

    Output
    ------
    file: text file to Updates folder
    """

    df_master = pd.DataFrame([])

    for key, value in file_dict.items():
        # read data
        df = pd.read_csv(value)

        # clean
        df = df.rename(
            columns={
                "fips": "Region Code",
                "county": "Region Name",
                "cases": "Cases",
                "deaths": "Deaths",
                "date": "Date",
            }
        )

        df["Region Name"] = df["Region Name"] + ", " + df["state"]
        df = df.drop(["state"], axis=1)

        df = pd.melt(
            df,
            id_vars=["Region Code", "Region Name", "Date"],
            var_name="Measure Name",
            value_name="Estimated Value",
            ignore_index=True,
        )

        df["Date"] = pd.to_datetime(df["Date"])
        df["Estimated Value"] = df["Estimated Value"].astype(float)

        # reset columns
        columns = [
            "Region Code",
            "Region Name",
            "Measure Name",
            "Date",
            "Estimated Value",
        ]
        df = df[columns]

        df["Region Code"] = df["Region Code"].astype(str)
        df["Region Code"] = df["Region Code"].str.rstrip(".0")
        df["Region Code"] = df["Region Code"].str.zfill(5)
        df.set_index("Region Code", inplace=True)

        df_master = df_master.append(df)

    df_master.to_csv("./" + file_location + "/Data/" + key + ".txt", sep="\t")
