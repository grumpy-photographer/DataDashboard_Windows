import pandas as pd


def geofred(file_dict, file_location):
    """
    Input
    -----
    file_dict: dictionary of files with key as filename and value as link to data
    file_location: folder location to store data

    Output
    ------
    file: text file to Updates folder
    """

    df_master = pd.DataFrame([])

    for key, value in file_dict.items():
        # Getting and reading new data
        df = pd.read_excel(value, skiprows=1)

        # Set Series ID as index
        df["Region Code"] = df["Region Code"].astype(str)
        df["Region Code"] = df["Region Code"].str.zfill(5)

        # create measure column
        df["Measure Name"] = key

        # Drop Series ID column
        df.drop("Series ID", axis=1, inplace=True)

        # Pivot data
        df = df.melt(
            id_vars=["Region Code", "Region Name", "Measure Name"],
            value_name="Estimated Value",
            var_name="Date",
        )
        df.set_index("Region Code", inplace=True)

        try:
            # Set Date column to date dtype
            df["Date"] = pd.to_datetime(df["Date"])
            df["Date"] = df["Date"].dt.date
        except Exception:
            df["Date"] = df["Date"].str.replace(" ", "-")
            df["Date"] = pd.to_datetime(df["Date"])
            df["Date"] = df["Date"].dt.date

        column_list = df.columns.values
        for i in column_list:
            df.loc[df[i].isnull(), i] = 0

        df_master = df_master.append(df)

    df_master.to_csv("./" + file_location + "/Data/" + key + ".txt", sep="\t")
