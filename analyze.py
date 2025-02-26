import pandas as pd
import matplotlib.pyplot as plt

js = pd.read_json('merged_payments_customers.json').fillna('not_disclosed')

summed_payments = js['amount'].sum()
print(summed_payments)

ids = set(js['customer_id'])
mean_amount = [float(js[js["customer_id"] == id]['amount'].mean()) for id in ids]
total_payments = js.groupby("customer_id")["amount"].sum()

# Sort in descending order and take top 5
top_5_customers = total_payments.sort_values(ascending=False).head(5)

# Add customer names (assuming first_name and last_name are consistent per customer_id)
customer_info = js[["customer_id", "first_name", "last_name"]].drop_duplicates()
top_5_with_names = top_5_customers.reset_index().merge(customer_info, on="customer_id")

# Display results
print("Top 5 Customers by Total Payment:")
print(top_5_with_names[["customer_id", "first_name", "last_name", "amount"]])



payment_type = js['payment_type'].value_counts()

payment_type.plot(kind="bar", title="№ of transactions by type", figsize=(6.40, 4.80))
plt.xticks(rotation=0)
plt.ylabel('№ of transactions', labelpad=8, fontsize=12)
plt.xlabel('Payment Type', labelpad=8, fontsize=12)
plt.show()

