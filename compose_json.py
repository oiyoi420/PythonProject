import pandas as pd

# Read the CSV files
df1 = pd.read_csv('customer.csv')
df2 = pd.read_csv('payment.csv')

# Merge the dataframes on the common column
merged_df = pd.merge(df1, df2, on='customer_id', how='outer')  # 'outer' keeps all rows, use 'inner' for only matching rows

# Convert to JSON and save
merged_df.to_json('merged_payments_customers.json', orient='records')

