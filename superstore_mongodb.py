import pandas as pd
from pymongo import MongoClient

# 1. Load CSV into MongoDB
client = MongoClient("mongodb://localhost:27017/")  # Change if using MongoDB Atlas
db = client["superstoreDB"]
orders_collection = db["Orders"]

# Load dataset
df = pd.read_csv("superstore.csv", encoding="ISO-8859-1")

# Convert DataFrame to dictionary and insert into MongoDB
orders_collection.delete_many({})  # clear old data
orders_collection.insert_many(df.to_dict(orient="records"))
print(" Data loaded into MongoDB.")

# 2. Retrieve and print all documents
print("\nAll documents in Orders collection:")
for doc in orders_collection.find():
    print(doc)

# 3. Count total documents
count_docs = orders_collection.count_documents({})
print(f"\nTotal number of documents: {count_docs}")

# 4. Orders from 'West' region
print("\nOrders from 'West' region:")
for doc in orders_collection.find({"Region": "West"}):
    print(doc)

# 5. Orders where Sales > 500
print("\nOrders with Sales > 500:")
for doc in orders_collection.find({"Sales": {"$gt": 500}}):
    print(doc)

# 6. Top 3 orders by Profit
print("\nTop 3 orders with highest Profit:")
for doc in orders_collection.find().sort("Profit", -1).limit(3):
    print(doc)

# 7. Update Ship Mode from "First Class" to "Premium Class"
update_result = orders_collection.update_many(
    {"Ship Mode": "First Class"},
    {"$set": {"Ship Mode": "Premium Class"}}
)
print(f"\nUpdated {update_result.modified_count} documents from First Class to Premium Class.")

# 8. Delete orders where Sales < 50
delete_result = orders_collection.delete_many({"Sales": {"$lt": 50}})
print(f"\nDeleted {delete_result.deleted_count} documents where Sales < 50.")

# 9. Aggregation: total sales per region
print("\nTotal sales per region:")
sales_per_region = orders_collection.aggregate([
    {"$group": {"_id": "$Region", "total_sales": {"$sum": "$Sales"}}}
])
for region in sales_per_region:
    print(region)

# 10. Distinct Ship Modes
distinct_modes = orders_collection.distinct("Ship Mode")
print("\nDistinct Ship Modes:", distinct_modes)

# 11. Number of orders per category
print("\nNumber of orders per category:")
orders_per_category = orders_collection.aggregate([
    {"$group": {"_id": "$Category", "count": {"$sum": 1}}}
])
for category in orders_per_category:
    print(category)
