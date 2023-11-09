#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pymongo
from pymongo import MongoClient


# In[2]:


client=MongoClient('localhost',27017)


# In[3]:


client.list_database_names()


# In[4]:


db=client.Amazon


# In[5]:


db.list_collection_names()


# In[6]:


import pandas as pd
df=pd.read_csv('AllYearSales.csv')
df.head(10)


# In[7]:


data=df.to_dict(orient="records")


# In[8]:


data


# In[9]:


# Access the "Amazon" database
db=client["Amazon"]


# In[10]:


print(db)


# In[11]:


collection = db.Amazon_Sales  # This line creates the collection
collection.insert_many(data) 


# In[12]:


db.Amazon.insert_many(data)


# In[13]:


db.list_collection_names()


# In[14]:


db.Amazon.count_documents({})


# In[15]:


cursor=db.Amazon.find({})
for doc in cursor: 
    print (doc)


# In[16]:


db.Amazon.find_one({'Order ID':176563})


# In[17]:


cursor=db['Amazon'].find({})


# In[18]:


for doc in cursor:
    print (doc)


# In[19]:


filter_criteria={'Order ID': '176558'}


# In[20]:


#Inserting one record at a time
db.Amazon.insert_one({'Order ID': 190939,
  'Product': 'USB-C Charging Cable',
  'Quantity Ordered': 2,
  'Price Each': 11.95,
  'Order Date': '4/19/2019 8:46',
  'City': ' San Jose',
  'State': 'California',
  'Category': 'Charging Cable'})


# In[21]:


#Inserting many records at a time
db.Amazon.insert_many([{'Order ID': 190940,
  'Product': 'Air-pods',
  'Quantity Ordered': 1,
  'Price Each': 199.95,
  'Order Date': '4/19/2019 8:46',
  'City': ' San Jose',
  'State': 'California',
  'Category': 'Headphones'},{'Order ID': 190941,
  'Product': 'lightning Charging Cable',
  'Quantity Ordered': 9,
  'Price Each': 9.95,
  'Order Date': '4/23/2019 8:46',
  'City': ' San Fransisco',
  'State': 'California',
  'Category': 'Charging Cable'}])


# In[22]:


db.Amazon.update_one({'Order ID': '190940'},{'$set':{'Order ID': '190942'}})


# In[23]:


#delete a record

filter_criteria = {'Order ID': 190941}

# Use the delete_one method to delete a single document based on the filter criteria
db.Amazon.delete_one(filter_criteria)


# ##### Analytics using Queries

# In[34]:


db=client["Amazon"]
# Query 1: Find all records in the Amazon_Sales collection
all_records = db['Amazon_Sales'].find()
df_all_records = pd.DataFrame(list(all_records))

# Display the query results
df_all_records.head()  # Display the first few records from the "all_records" query


# In[35]:


# Query 2: Find orders with a Quantity Ordered greater than 1
high_quantity_orders = db['Amazon_Sales'].find({"Quantity Ordered": {"$gt": 1}})
df_high_quantity_orders = pd.DataFrame(list(high_quantity_orders))

# Display the query results
df_high_quantity_orders.head()  # Display the first few records from the "high_quantity_orders" query


# In[38]:


# Query 3: Find orders from a specific city, e.g., "Los Angeles"
los_angeles_orders = db['Amazon_Sales'].find({"City": "Los Angeles"})
df_los_angeles_orders = pd.DataFrame(list(los_angeles_orders))


# Display the query results
df_los_angeles_orders.head()  # Display the first few records from the "los_angeles_orders" query


# In[37]:


# Query 4: Find orders with a specific product, e.g., "Google Phone"
google_phone_orders = db['Amazon_Sales'].find({"Product": "Google Phone"})
df_google_phone_orders = pd.DataFrame(list(google_phone_orders))

# Display the query results
df_google_phone_orders.head()  # Display the first few records from the "google_phone_orders" query


# In[27]:


#Total revenue from Phone category in April 2019
pipeline = [
    {
        "$match": {
            "Category": "Phone",
            "Order Date": {
                "$gte": "4/1/2019 0:00",
                "$lte": "4/30/2019 23:59"
            }
        }
    },
    {
        "$project": {
            "Revenue": { "$multiply": ["$Quantity Ordered", "$Price Each"] }
        }
    },
    {
        "$group": {
            "_id": None,
            "Total Revenue": { "$sum": "$Revenue" }
        }
    }
]

result = list(db['Amazon_Sales'].aggregate(pipeline))

# Check if the result list is not empty before accessing elements
if result:
    total_revenue = result[0]["Total Revenue"]
    print(f'Total revenue from Phone category in April 2019: ${total_revenue:.2f}')
else:
    print('No results found for the specified conditions.')


# In[28]:


#Find top-selling products by revenue
pipeline = [
    {
        "$project": {
            "Product": 1,
            "Revenue": { "$multiply": ["$Quantity Ordered", "$Price Each"] }
        }
    },
    {
        "$group": {
            "_id": "$Product",
            "Total Revenue": { "$sum": "$Revenue" }
        }
    },
    {
        "$sort": { "Total Revenue": -1 }
    },
    {
        "$limit": 10  # Adjust the limit to get the top N products
    }
]

result = list(db['Amazon_Sales'].aggregate(pipeline))

# Display the top-selling products by revenue
if result:
    print("Top-selling products by revenue:")
    for item in result:
        print(f'{item["_id"]}: ${item["Total Revenue"]:.2f}')
else:
    print('No results found for the specified conditions.')


# In[29]:


# query to display the total sales revenue for each state, sorted in descending order, to identify 
#the states with the highest sales revenue.

pipeline = [
    {
        "$project": {
            "State": 1,
            "Revenue": { "$multiply": ["$Quantity Ordered", "$Price Each"] }
        }
    },
    {
        "$group": {
            "_id": "$State",
            "Total Revenue": { "$sum": "$Revenue" }
        }
    },
    {
        "$sort": { "Total Revenue": -1 }
    }
]

result = list(db['Amazon_Sales'].aggregate(pipeline))

# Display the total sales revenue by state
if result:
    print("Total sales revenue by state:")
    for item in result:
        print(f'{item["_id"]}: ${item["Total Revenue"]:.2f}')
else:
    print('No results found for the specified conditions.')


# #### Aggregration 

# In[33]:


#Calculate average order value for each product category
pipeline = [
    {
        "$project": {
            "Category": 1,
            "Revenue": { "$multiply": ["$Quantity Ordered", "$Price Each"] }
        }
    },
    {
        "$group": {
            "_id": "$Category",
            "Total Revenue": { "$sum": "$Revenue" },
            "Total Orders": { "$sum": 1 }
        }
    },
    {
        "$project": {
            "_id": 1,
            "Average Order Value": { "$divide": ["$Total Revenue", "$Total Orders"] }
        }
    }
]

result = list(db['Amazon_Sales'].aggregate(pipeline))

# Display the average order value for each product category
if result:
    print("Average order value for each product category:")
    for item in result:
        print(f'{item["_id"]}: ${item["Average Order Value"]:.2f}')
else:
    print('No results found for the specified conditions.')


# In[ ]:




