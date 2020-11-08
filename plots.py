import glob
import pandas as pd
import plotly.express as px

def plot_scripts(data_folder):
    """
    Inputs
    ------
    data_folder : str : folder where data is located

    Outputs
    -------
    graphs of data from data_folder
    """
    for x in glob.glob(data_folder):
        if "Data_Series" in x:
            df = pd.read_csv(x, sep="\t", low_memory=False)

            melt_df = df.melt(
                id_vars=[
                    "GeoArea_FIPS",
                    "GeoArea_Name",
                    "Economic_Measure_Code",
                    "Economic_Measure_Name",
                    "Published_UOM",
                    "Calculation_Type",
                    "Default_Scale",
                ],
                var_name="Date",
                value_name="Published_Value",
            )
            
            df["GeoArea_FIPS"] = df["GeoArea_FIPS"].astype(str)
            df["Published_Value"] = df["Published_Value"].astype(float)

    fig = px.scatter(melt_df, x="GeoArea_FIPS", y="Published_Value", color="GeoArea_Name")
    
    return fig
