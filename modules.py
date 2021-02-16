def housing(file_dict):
    """
    Input
    -----
    dict: dictionary of files with key as filename and value as link to data

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

        df.to_csv("./Updates/" + key + ".txt", sep="\t")


def geofred(file_dict):
    """
    Input
    -----
    dict: dictionary of files with key as filename and value as link to data

    Output
    ------
    file: text file to Updates folder
    """

    for key, value in file_dict.items():
        # Getting and reading new data
        df = pd.read_excel(
            value,
            skiprows=1,
        )

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

        # Set Date column to date dtype
        df["Date"] = pd.to_datetime(df["Date"])
        df["Date"] = df["Date"].dt.date

        column_list = df.columns.values
        for i in column_list:
            df.loc[df[i].isnull(), i] = 0

        df.to_csv("./Updates/" + key + ".txt", sep="\t")


def unzip(file_dict):
    """
    Input
    -----
    dict: dictionary of files with key as filename and value as link to data

    Output
    ------
    file: text file to Updates folder
    """

    for key, value in file_dict.items():
        response = requests.get(value)
        zip_file = ZipFile(BytesIO(response.content))
        files = zip_file.namelist()

        for file in files:
            if ".csv" in file and "ALL" not in file or ".csv" in file:
                with zip_file.open(file) as csvfile:
                    df = pd.read_csv(
                        csvfile, encoding="ISO-8859-1", sep=",", low_memory=False
                    )

                    df = df.rename(
                        columns={
                            "GeoFIPS": "Region Code",
                            "Description": "Measure_Name",
                            "GeoName": "Region Name",
                        }
                    )

                    df["LineCode"] = df["LineCode"].astype("str")

                    # Remove quotes from GeoFIPS
                    df["Region Code"] = df["Region Code"].str.replace('"', "")

                    # Drop rows at the end of table
                    df.drop(df.tail(4).index, inplace=True)

                    # Remove rows that are not needed
                    df = df.drop(
                        columns=[
                            "Region",
                            "TableName",
                            "LineCode",
                            "IndustryClassification",
                            "Unit",
                        ]
                    )

                    # Strip whitespace from object type columns
                    df["Measure_Name"] = df["Measure_Name"].str.replace("2/", "")
                    df["Measure_Name"] = df["Measure_Name"].str.replace("7/", "")
                    df_obj = df.select_dtypes(["object"])
                    df[df_obj.columns] = df_obj.apply(lambda x: x.str.strip())

                    measures = [
                        "Agriculture, forestry, fishing and hunting",
                        "Mining, quarrying, and oil and gas extraction",
                        "Utilities",
                        "Construction",
                        "Manufacturing",
                        "Wholesale trade",
                        "Retail trade",
                        "Transportation and warehousing",
                        "Information",
                        "Finance and insurance",
                        "Real estate and rental and leasing",
                        "Professional and business services",
                        "Management of companies and enterprises",
                        "Administrative and support and waste management and remediation services",
                        "Educational services",
                        "Health care and social assistance",
                        "Arts, entertainment, and recreation",
                        "Accommodation and food services",
                        "Other services (except government and government enterprises)",
                        "Government and government enterprises",
                    ]

                    df = df.query("Measure_Name in @measures")
                    df = df.rename(columns={"Measure_Name": "Measure Name"})

                    df["Measure Name"] = df["Measure Name"].str.replace("2/", "")
                    df["Measure Name"] = df["Measure Name"].str.replace("7/", "")

                    df = df.melt(
                        id_vars=["Region Code", "Region Name", "Measure Name"],
                        var_name="Date",
                        value_name="Estimated Value",
                    )

                    df["Estimated Value"] = df["Estimated Value"].replace("(D)", "")

                    df.dropna(inplace=True)

                    columns = [
                        "Region Code",
                        "Region Name",
                        "Measure Name",
                        "Date",
                        "Estimated Value",
                    ]

                    df = df[columns]

                    df["Region Code"] = df["Region Code"].str.lstrip()

                    df.set_index("Region Code", inplace=True)

                    df["Date"] = pd.to_datetime(df["Date"])
                    df["Date"] = df["Date"].dt.date

                    df.to_csv("./Updates/" + key + ".txt", sep="\t")
