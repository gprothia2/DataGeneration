#!/usr/bin/env python
# coding: utf-8

# In[181]:


import random
import time
import json
import pandas as pd
import datetime
import awswrangler as wr


# In[182]:


customer=['C-01','C-02','C-03','C-04','C-05','C-06','C-07',]
order = ['O-01','O-02','O-02','O-03','O-04','O-05','O-06','O-07']
zip = ['94582','94583','94586','94587']
supplier = ['S-01','S-02','S-03']
product=['P-01','P-02','P-03','P-04','P-05','P-06','P-07']
product_item = ['I-01','I-02','I-03']
customer_tier = ['1','2','3']
events = ['Storm','Heatwave','Wildfire']
get_ipython().system('aws s3 rm s3://data-gap/yourCustName --recursive')
df = wr.athena.start_query_execution(sql = "drop database if exists yourCustName  cascade", database="yourCustName", wait=True)
df = wr.athena.start_query_execution(sql = "create database yourCustName ", database="yourCustName", wait=True)


# In[190]:


# Customer Data generation
record={}
df = pd.DataFrame(record)
for x in customer:
  record['customer_id'] = x
  record['name'] = record['customer_id'] + '-' + 'name'
  record['ship_location'] = random.choice(zip)
  record['customer_tier'] = random.choice(customer_tier)
    
  df2 = record
  df = df.append(df2, ignore_index = True)
wr.s3.to_parquet(df=df,path='s3://data-gap/yourCustName/customer/customer.parquet',  dataset=True ,
                 index=False,database='yourCustName',table ='customer',mode="overwrite")


# In[191]:


# Product and BOM data generation
record={}

df = pd.DataFrame(record)
for x in product:
  seq=0
  for y in product_item:
    seq = seq + 1
    record['product_id'] = x
    record['name'] = record['product_id'] + '-' + 'name'
    record['item_id'] = record['product_id'] + '-' + y
    record['supplier_id'] = random.choice(supplier)
    record['build_time'] = int(random.randint(1,10))
    record['lead_time'] = int(random.randint(1,10))
    record['seq'] = seq
    
    df2 = record
    df = df.append(df2, ignore_index = True)
wr.s3.to_parquet(df=df,path='s3://data-gap/yourCustName/product/product.parquet',  dataset=True ,
                 index=False,database='yourCustName',table = 'product',mode="overwrite")


# In[192]:


# Supplier data generation
record={}
df = pd.DataFrame(record)
for x in supplier:
    record['supplier_id'] = x
    record['name'] = record['supplier_id'] + '-' + 'name'
    record['location'] = random.choice(zip)
    df2 = record
    df = df.append(df2, ignore_index = True)
wr.s3.to_parquet(df=df,path='s3://data-gap/yourCustName/supplier/supplier.parquet',  dataset=True ,
                 index=False,database='yourCustName',table = 'supplier',mode="overwrite")


# In[193]:


# Order data Generation
record={}
df = pd.DataFrame(record)
for x in range(10000):
  record['customer_id'] = random.choice(customer)
  record['order_id'] = record['customer_id'] + '-' + str(x)
  record['product_id'] = random.choice(product)
  record['revenue'] = int(random.randint(1000,70000))
  record['profit'] = int(random.randint(100,1000))
  record['delivery_date'], record['ship_to_date']  = random_date()
    
  df2 = record
  df = df.append(df2, ignore_index = True)
wr.s3.to_parquet(df=df,path='s3://data-gap/yourCustName/order/order.parquet',  dataset=True ,
                 index=False,database='yourCustName',table = 'orders',mode="overwrite")


# In[194]:


def random_date():
    start_dt = datetime.date(2022, 9, 1)
    end_dt = datetime.date(2022, 9, 30)
    time_between_dates = end_dt - start_dt
    days_between_dates = time_between_dates.days
    random_number_of_days = random.randrange(days_between_dates)
    random_date = start_dt + datetime.timedelta(days=random_number_of_days)
    delivery_date = random_date
    ship_to_date = random_date + datetime.timedelta(days=-30)
   # print(random_date,weekday)
    return delivery_date,ship_to_date


# In[202]:


# Event data generation
get_ipython().system('aws s3 rm s3://data-gap/yourCustName/event --recursive')

record={}
df = pd.DataFrame(record)
for x in events:
    record['name'] = x
    record['end_date'],record['start_date'] = random_date()
    record['location'] = random.choice(zip)
    df2 = record
    df = df.append(df2, ignore_index = True)
wr.s3.to_csv(df=df,path='s3://data-gap/yourCustName/event/event.csv',  dataset=True ,
                 index=False,database='yourCustName',table = 'event',mode="overwrite")


# In[204]:


data_sets_list = ['event_impact_order_v','event_impact_work_order_v','event_impact_order_details']

for i in range(len(data_sets_list)):
   data_set = data_sets_list[i]
   ingestion_id = wr.quicksight.create_ingestion(data_set)
   while wr.quicksight.describe_ingestion(ingestion_id=ingestion_id,dataset_name=data_set)['IngestionStatus'] not in ["COMPLETED", "FAILED"]:
     time.sleep(10)


# In[ ]:



