# https://github.com/Kaggle/kagglehub/blob/main/README.md#kaggledatasetadapterpandas
import kagglehub
from kagglehub import KaggleDatasetAdapter
import sqlalchemy
import pandas as pd
import numpy as np

#initialise variables
dataset_name = 'computingvictor/transactions-fraud-datasets' # dataset name from kaggle
table_name = 'transactions_data'  # Name of the SQL table to save the data
slice_size = 10000
dataframe_size_limit = 100000  # Limit to the first 100,000 rows
data_n_days_ago = 7  # Number of days of data to load

def get_existing_sql_data(table_name):
    engine = connect_to_sql()
    query = f"SELECT * FROM {table_name}"
    df = pd.read_sql(query, engine)
    
    return df

def get_data_from_kaggle(dataset_name, table_name, data_n_days_ago):
    df = kagglehub.load_dataset(
        KaggleDatasetAdapter.PANDAS,
        dataset_name,
        table_name + '.csv',
        sql_query = f"SELECT * FROM {table_name} WHERE date >= GETDATE() - {data_n_days_ago}"
    )

    return df

def combine_data(existing_df, new_df):
    max_date_old_data = existing_df['date'].max()
    min_date_new_data = new_df['date'].min()
    
    if max_date_old_data and min_date_new_data and max_date_old_data < min_date_new_data:
        print('Warning: There is a gap between the existing data and the new data. Increase value of "data_n_days_ago" to resolve this. Stop script?')
        stop_script = input()
        if stop_script.lower() in ['y', 'yes']:
            exit()

    df = pd.concat([new_df, existing_df]).drop_duplicates(keep = 'first').reset_index(drop = True)
    
    return df

def remove_currency(amount_str):
    if amount_str:
        return float(amount_str.replace('$', ''))
    
    return amount_str

def connect_to_sql():
    sql_connection_string = (
        'mssql+pyodbc://localhost\\SQLEXPRESS/kaggle'
        '?driver=ODBC+Driver+17+for+SQL+Server'
        '&server=localhost' # or 'NICS_LAPTOP\SQLEXPRESS'
        '&trusted_connection=yes'
    )

    engine = sqlalchemy.create_engine(sql_connection_string)
    engine.connect()  # Establish the connection

    return engine

def clean_dataframe(df):
    df['amount'] = df['amount'].replace('$' ,'')  # Remove the dollar sign from the 'amount' column
    df['date'] = pd.to_datetime(df['date'], errors = 'coerce')  # Convert 'date' column to datetime format

    df = df.replace(r'^\s*$', np.nan, regex = True)

    return df

def save_to_sql_in_slices(slice_size, df, table_name):
    engine = connect_to_sql()

    for i in range(0, len(df), slice_size):

        ifexists = 'append' if i > 0 else 'replace'  # Use 'replace' for the first chunk, 'append' for subsequent chunks
    
        sliced_df = df.iloc[i : i + slice_size]  # Slice the DataFrame to get one row at a time
        sliced_df.to_sql(
            table_name,
            con = engine,
            if_exists = ifexists,
            index = False,
        )

old_df  = get_existing_sql_data(table_name) #Get existing data from SQL table
new_df = get_data_from_kaggle(dataset_name, table_name, data_n_days_ago) # Get new data from Kaggle
df = combine_data(old_df, new_df) # Combine old and new data

df = clean_dataframe(df)
save_to_sql_in_slices(slice_size, df, table_name) # Save the DataFrame to a SQL table