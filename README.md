# DataGeneration
Python/pandas script for data generation

This script requires installing awswrangler.
The script is sample script to generate Customers, orders, Products, BOM, Supplier and Weather Events with dates.

Update the script to align with the data you need for yoru specific sceario.  This script will generate the data files, 
and upload it to S3 bucket and under specific prefixes. It will also create the Glue table that can be queried using Athena and SQL
