import pandas as pd
import requests
import zipfile
from io import BytesIO


def bea(file_dict, file_location):
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
        response = requests.get(value)
        zip_file = zipfile.ZipFile(BytesIO(response.content))
        files = zip_file.namelist()

        for file in files:
            if ".csv" in file and "ALL" not in file or ".csv" in file:
                with zip_file.open(file) as csvfile:
                    df = pd.read_csv(
                        csvfile, encoding="ISO-8859-1", sep=",", low_memory=False)

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

                    df_master = df_master.append(df)

                df_master.to_csv("./" + file_location + "/Data/" + key + ".txt", sep="\t")
