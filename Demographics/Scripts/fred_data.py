
import pandas as pd
import glob 

for file in glob.glob("../Updates/*.txt"):

    # create empty dataframe
    df = pd.DataFrame()

    # read text files
    dataframe = pd.read_csv(file, sep="\t")

    # ensure Region Code column is str and has 5 digits
    dataframe["Region Code"] = dataframe["Region Code"].astype(str)
    dataframe["Region Code"] = dataframe["Region Code"].str.zfill(5)

    # set index
    dataframe.set_index("Region Code", inplace=True)

    # append data to empty dataframe
    data = df.append(dataframe)

# save data to text file
data.to_csv("../Updates/demographics.txt", sep="\t")