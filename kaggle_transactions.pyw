import useful_functions as uf
import sqlalchemy as sqla

#initialise variables
slice_size = int(1E4)
dataframe_size_limit = int(1E5)  # Limit to the first 100,000 rows
data_n_days_ago = 2  # Number of days of data to load
dataset_name = 'computingvictor/transactions-fraud-datasets' # dataset name from kaggle

# Transactions
# source_table_name = 'transactions_data'
# sql_table_name = 'transactions'  # Name of the SQL table to save the data
# table_type = 'fact'
# datatypes = {
#     'id': sqla.BigInteger,
#     'date': sqla.Date,
#     'client_id': sqla.BigInteger,
#     'card_id': sqla.BigInteger,
#     'amount': sqla.Float,
#     'use_chip': sqla.Text,
#     'merchant_id': sqla.BigInteger,
#     'merchant_city': sqla.Text,
#     'merchant_state': sqla.Text,
#     'zip': sqla.Integer,
#     'mcc': sqla.BigInteger,
#     'errors': sqla.Text,
#     }
# uf.get_new_old_combine_clean_save(dataset_name, datatypes, table_type, data_n_days_ago, source_table_name, sql_table_name, slice_size)

# cards_dat
source_table_name = 'cards_data'
sql_table_name = 'card'  # Name of the SQL table to save the data
table_type = 'dim'
datatypes = {
    'id': sqla.BigInteger,
    'client_id': sqla.BigInteger,
    'card_brand': sqla.Text,
    'card_type': sqla.Text,
    'card_number': sqla.BigInteger,
    'expires': sqla.Text,
    'cvv': sqla.Integer,
    'has_chip': sqla.Boolean,
    'num_cards_issued': sqla.Integer,
    'credit_limit': sqla.Float,
    }
uf.get_new_old_combine_clean_save(dataset_name, datatypes, table_type, data_n_days_ago, source_table_name, sql_table_name, slice_size)

# # Transactions
# dataset_name = 'computingvictor/mcc_codes' # dataset name from kaggle
# table_name = 'merchant'  # Name of the SQL table to save the data
# uf.get_new_old_combine_clean_save(dataset_name, data_n_days_ago, table_name, slice_size)

# # Transactions
# dataset_name = 'computingvictor/users_data' # dataset name from kaggle
# table_name = 'user'  # Name of the SQL table to save the data
# uf.get_new_old_combine_clean_save(dataset_name, data_n_days_ago, table_name, slice_size)