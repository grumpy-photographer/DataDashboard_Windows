import pandas as pd

# create empty dataframe
df = pd.DataFrame()

# read civilian labor force data
df2 = pd.read_csv("../Updates/FRED_Civilian_Labor_Force_by_County_Persons.txt", sep="\t")

# read eqfxsubprime data
df3 = pd.read_csv("../Updates/FRED_EQFXSUBPRIME.txt", sep="\t")

# read education data
df4 = pd.read_csv("../Updates/FRED_People_25_Years_and_Over_Who_Have_Completed_an_Associates_Degree_or_Higher_5year_estimate_by_County_Percent.txt", sep="\t")

# read pop data
df5 = pd.read_csv("../Updates/FRED_Resident_Population_by_County_Thousands_of_Persons.txt", sep="\t")

# append data
df_list = [df2, df3, df4, df5]
data = df.append(df_list)

# make sure region code is str and set index
data["Region Code"] = data["Region Code"].astype(str)
data["Region Code"] = data["Region Code"].str.zfill(5)
data = data.set_index("Region Code")

# save data to text file
data.to_csv("../Updates/demographics.txt", sep="\t")
