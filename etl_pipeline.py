#!/usr/bin/env python
# coding: utf-8

# # ETL Pipeline Preparation
# Follow the instructions below to help you create your ETL pipeline.
# ### 1. Import libraries and load datasets.
# - Import Python libraries
# - Load `messages.csv` into a dataframe and inspect the first few lines.
# - Load `categories.csv` into a dataframe and inspect the first few lines.

# import libraries
import pandas as pd
import re
from sqlalchemy import create_engine

# load messages dataset
messages = pd.read_csv('messages.csv')
messages.head()

# load categories dataset
categories = pd.read_csv('categories.csv')

# split entry data on ';'
categories = categories['categories'].str.split(';', expand=True)

# create columns names from entry data
columns = [item.replace("0", "").replace("-","").replace("1","") for item in list(categories.iloc[0])]

#rename dataframe columns
categories.columns = columns

# filter text data from data entries in dataframe
categories = categories.applymap(lambda x: re.sub("[^0-9]", "", x))
categories.head()

# ### 2. Merge datasets.
# - Merge the messages and categories datasets using the common id
# - Assign this combined dataset to `df`, which will be cleaned in the following steps

# merge datasets
df = pd.concat([messages, categories], axis=1, join="inner")
df.head(2)

# ### 3. Split `categories` into separate category columns.
# - Split the values in the `categories` column on the `;` character so that each value becomes a separate column. You'll find [this method](https://pandas.pydata.org/pandas-docs/version/0.23/generated/pandas.Series.str.split.html) very helpful! Make sure to set `expand=True`.
# - Use the first row of categories dataframe to create column names for the categories data.
# - Rename columns of `categories` with new column names.

print("done in preprocessing phase")

# ### 4. Convert category values to just numbers 0 or 1.
# - Iterate through the category columns in df to keep only the last character of each string (the 1 or 0). For example, `related-0` becomes `0`, `related-1` becomes `1`. Convert the string to a numeric value.
# - You can perform [normal string actions on Pandas Series](https://pandas.pydata.org/pandas-docs/stable/text.html#indexing-with-str), like indexing, by including `.str` after the Series. You may need to first convert the Series to be of type string, which you can do with `astype(str)`.

print("done in preprocessing phase")


# ### 5. Replace `categories` column in `df` with new category columns.
# - Drop the categories column from the df dataframe since it is no longer needed.
# - Concatenate df and categories data frames.

print("done in preprocessing phase")

# ### 6. Remove duplicates.
# - Check how many duplicates are in this dataset.
# - Drop the duplicates.
# - Confirm duplicates were removed.

# check number of duplicates
df_no_duplicates = df.drop_duplicates()
print(f'number of duplicates: {len(df)-len(df_no_duplicates)}')

# drop duplicates

df = df_no_duplicates.copy()

# check number of duplicates

print(f'number of duplicates: {len(df)-len(df_no_duplicates)}')

# ### 7. Save the clean dataset into an sqlite database.
# You can do this with pandas [`to_sql` method](https://pandas.pydata.org/pandas-docs/stable/generated/pandas.DataFrame.to_sql.html) combined with the SQLAlchemy library. Remember to import SQLAlchemy's `create_engine` in the first cell of this notebook to use it below.

engine = create_engine('sqlite:///InsertDatabaseName.db')
df.to_sql('InsertTableName', engine, index=False)

# ### 8. Use this notebook to complete `etl_pipeline.py`
# Use the template file attached in the Resources folder to write a script that runs the steps above to create a database based on new datasets specified by the user. Alternatively, you can complete `etl_pipeline.py` in the classroom on the `Project Workspace IDE` coming later.