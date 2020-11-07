import os
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
    for x in os.listdir(data_folder):
        df = pd.read_csv(x)

        #px.bar(data_frame=df, x="Data_Period_Business_Key", y="Estimated_Value")
        print(df)