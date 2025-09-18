# https://github.com/Kaggle/kagglehub/blob/main/README.md#kaggledatasetadapterpandas
from ast import Try
import kagglehub
from kagglehub import KaggleDatasetAdapter
import sqlalchemy
import pandas as pd
import numpy as np

def connect_to_sql():
    sql_connection_string = (
        'mssql+pyodbc://localhost\\SQLEXPRESS/kaggle'
        '?driver=ODBC+Driver+17+for+SQL+Server'
        '&server=localhost' # or 'NICS_LAPTOP\SQLEXPRESS'
        '&trusted_connection=yes'
    )

    engine = sqlalchemy.create_engine(sql_connection_string)

    return engine

def get_existing_sql_data(table_name):
    engine = connect_to_sql()
    try:
        query = f"SELECT * FROM {table_name}"
        df = pd.read_sql(query, engine)
        return df
    except:
        print('Table "' + table_name + '" doesn\'t exist yet')
        return
    
def get_data_from_kaggle(dataset_name, datatypes, source_table_name, table_type, data_n_days_ago):
    date_columns = [col for col, dtype in datatypes.items() if dtype == sqlalchemy.Date]
    
    if table_type == 'fact':
        sql_query = f"SELECT * FROM {source_table_name} WHERE {date_columns[0]} >= GETDATE() - {data_n_days_ago}"
    else:
        sql_query = f"SELECT * FROM {source_table_name}"

    try:
        df = kagglehub.load_dataset(
            KaggleDatasetAdapter.PANDAS,
            dataset_name,
            source_table_name + '.csv',
            sql_query = sql_query
        )

        if df is not None:
            df = clean_dataframe(df)
            df = set_datatypes(df, datatypes)
            
        return df

    except Exception as e:
        print(f'Error getting data from Kaggle: {e}')
        return

def isolate_new_data(existing_df, new_df):
    max_date_old_data = existing_df['date'].max()
    min_date_new_data = new_df['date'].min()
    
    if max_date_old_data and min_date_new_data and max_date_old_data < min_date_new_data:
        print('Warning: There is a gap between the existing data and the new data. Increase value of "data_n_days_ago" to resolve this. Stop script?')
        stop_script = input()
        if stop_script.lower() in ['y', 'yes']:
            exit()

    merged_df = pd.concat([new_df, existing_df]).drop_duplicates(keep = 'first') # Assuming 'id' is the unique identifier column in every table
    df = new_df[~new_df['id'].isin(merged_df['id'])] # Keep only rows in new_df that are not in existing_df based on 'id'
    
    return df

def clean_float_column(float_str):
    if float_str:
        float_str = str(float_str).replace('$', '').strip()
        
    return float_str

def set_datatypes(df, datatypes):
    for column, dtype in datatypes.items():
        if dtype == sqlalchemy.Float:
            df[column] = pd.to_numeric(df[column].apply(clean_float_column), errors = 'coerce', downcast = 'float')
        elif dtype == sqlalchemy.Date:
            df[column] = pd.to_datetime(df[column], errors = 'coerce')
        elif dtype == sqlalchemy.BigInteger or dtype == sqlalchemy.Integer:
            df[column] = pd.to_numeric(df[column], errors = 'coerce', downcast = 'integer')

    return df

def clean_dataframe(df):
    df = df.replace(r'^\s*$', np.nan, regex = True)

    return df

def save_to_sql_in_slices(slice_size, df, table_name, datatypes): #Only save new data and append. Note: This means no modifications will be made in the SQL table from the new data.
    engine = connect_to_sql()

    for i in range(0, len(df), slice_size):
        sliced_df = df.iloc[i : i + slice_size]  # Slice the DataFrame to get n rows at a time
        sliced_df.to_sql(
            table_name,
            con = engine,
            if_exists = 'append',
            index = None,
            dtype = datatypes
        )

    return

def get_new_old_combine_clean_save(dataset_name, datatypes, table_type, data_n_days_ago, source_table_name, sql_table_name, slice_size):
    old_df  = get_existing_sql_data(sql_table_name) # Get existing data from SQL table
    if old_df is not None:
        print('Existing data: ' + str(len(old_df)))
        print(old_df.head())
    
    new_df = get_data_from_kaggle(dataset_name, datatypes, source_table_name, table_type, data_n_days_ago) # Get latest n days of data from Kaggle
    if new_df is not None:
        print('New data: ' + str(len(new_df)))
        print(new_df.head())
    else:
        print('Error: new data not available from Kaggle')
        return
    
    if old_df is not None and table_type == 'fact':
        print('Isolating only the new and data (avoiding duplicating data in SQL)')
        new_df = isolate_new_data(old_df, new_df) # Combine old and new data
        print('Isolated new data: ' + str(len(new_df)))
        print(new_df.head())
    elif table_type == 'fact':
        print('No old data - saving new data to SQL')

    if new_df is not None:
        save_to_sql_in_slices(slice_size, new_df, sql_table_name, datatypes) # Save the DataFrame to a SQL table
    else:
        print('No new data to save to SQL')

    return