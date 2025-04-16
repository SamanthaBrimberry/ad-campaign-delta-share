# Databricks notebook source
# DBTITLE 1,Delta Share Presentation
displayHTML(f'''<div style="width:1150px; margin:auto"><iframe src="https://docs.google.com/presentation/d/1ZwMzYHCghtVdzCB0bViMzzeQGfyhZQix/embed?slide=10" frameborder="0" width="1150" height="683"></iframe></div>''')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Delta Sharing
# MAGIC
# MAGIC Delta Sharing let you share data with external recipient without creating copy of the data. Once they're authorized, recipients can access and download your data directly.
# MAGIC
# MAGIC In Delta Sharing, it all starts with a Delta Lake table registered in the Delta Sharing Server by the data provider. <br/>
# MAGIC This is done with the following steps:
# MAGIC - Create a RECIPIENT and share activation link with your recipient 
# MAGIC - Create a SHARE
# MAGIC - Add your Delta tables to the given SHARE
# MAGIC - GRANT SELECT on your SHARE to your RECIPIENT
# MAGIC  
# MAGIC Once this is done, your customer will be able to download the credential files and use it to access the data directly:
# MAGIC
# MAGIC - Client authenticates to Sharing Server
# MAGIC - Client requests a table (including filters)
# MAGIC - Server checks access permissions
# MAGIC - Server generates and returns pre-signed short-lived URLs
# MAGIC - Client uses URLs to directly read files from object storage
# MAGIC <br>
# MAGIC <img src="https://raw.githubusercontent.com/QuentinAmbard/databricks-demo/main/product_demos/delta-sharing-flow.png" width="1000" />
# MAGIC
# MAGIC ## Unity Catalog
# MAGIC Databricks Unity Catalog is the central place to administer your data governance and security.<br/>
# MAGIC Unity Catalog’s security model is based on standard ANSI SQL, to grant permissions at the level of databases, tables, views, rows and columns<br/>
# MAGIC Using Databricks, we'll leverage the Unity Catalog to easily share data with our customers.

# COMMAND ----------

# DBTITLE 1,Setting Up Widgets for Data Sharing in Databricks
# MAGIC %py
# MAGIC catalog = 'sb'
# MAGIC schema = 'ads'
# MAGIC share_d2o = 'campaign_kpi_open'
# MAGIC share_d2d = 'campaign_kpi_databricks'
# MAGIC account_users = 'sammy.brimberry@databricks.com'
# MAGIC
# MAGIC dbutils.widgets.text('catalog', catalog)
# MAGIC dbutils.widgets.text('schema', schema)
# MAGIC dbutils.widgets.text('share_d2o', share_d2o)
# MAGIC dbutils.widgets.text('share_d2d', share_d2d)
# MAGIC dbutils.widgets.text('account_users', account_users)

# COMMAND ----------

# DBTITLE 1,Creating and Using a Catalog and Schema in SQL
# MAGIC %sql
# MAGIC create catalog if not exists ${catalog};
# MAGIC use catalog ${catalog};
# MAGIC create schema if not exists ${schema};

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1: Create Share
# MAGIC We will create 2 shares for different client scenarios.

# COMMAND ----------

# DBTITLE 1,Create Campaign Data Shares
# MAGIC %sql
# MAGIC -- open
# MAGIC create share if not exists ${share_d2o}
# MAGIC comment "campaign data aggregated by campagin id provided by Sammy, to Sammy's laptop for Databricks Demos";
# MAGIC -- databricks
# MAGIC create share if not exists ${share_d2d}
# MAGIC comment "campaign data aggregated by campagin id provided by Sammy, to Sammy's GCP Databricks Workspace for Databricks Demos";
# MAGIC
# MAGIC -- grant access to users who will manage these shares
# MAGIC alter share ${share_d2o} owner to `${account_users}`;
# MAGIC alter share ${share_d2d} owner to `${account_users}`;

# COMMAND ----------

# DBTITLE 1,Describing Data Share
# MAGIC %sql
# MAGIC describe share ${share_d2d};

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Managing our Share(s)

# COMMAND ----------

# DBTITLE 1,KPI Table to Share
# MAGIC %sql
# MAGIC select * from ${catalog}.${schema}.campaign_kpis;

# COMMAND ----------

# DBTITLE 1,Add Partition to Tables in Share
# MAGIC %sql
# MAGIC -- We plan to share specific partions of files later on... 
# MAGIC -- Partition KPI and raw tables as required
# MAGIC CREATE OR REPLACE TABLE ${catalog}.${schema}.campaign_kpis 
# MAGIC USING delta
# MAGIC PARTITIONED BY (campaign_id)
# MAGIC AS SELECT * FROM ${catalog}.${schema}.campaign_kpis;
# MAGIC
# MAGIC CREATE OR REPLACE TABLE ${catalog}.${schema}.campaign_raw 
# MAGIC USING delta
# MAGIC PARTITIONED BY (campaign_id, exposure_year)
# MAGIC AS SELECT *,
# MAGIC year(exposure_timestamp) as exposure_year
# MAGIC FROM ${catalog}.${schema}.campaign_raw;

# COMMAND ----------

# DBTITLE 1,Adding a Viewers Table to D2O Share
# MAGIC %sql
# MAGIC -- alter share ${share_d2o} add table ${catalog}.${schema}.ads; 
# MAGIC alter share ${share_d2o} add table ${catalog}.${schema}.viewers

# COMMAND ----------

# DBTITLE 1,Adding Campaign KPIs Table to Shares
# MAGIC %sql
# MAGIC alter share ${share_d2o} add table ${catalog}.${schema}.campaign_kpis;
# MAGIC alter share ${share_d2d} add table ${catalog}.${schema}.campaign_kpis;

# COMMAND ----------

# DBTITLE 1,Mange Shares
# MAGIC %sql
# MAGIC -- Add files to share that match partition specs
# MAGIC alter share ${share_d2d}
# MAGIC add table ${catalog}.${schema}.campaign_raw
# MAGIC partition (campaign_id = "CAMP032")
# MAGIC as ${schema}.campaign_kpis_CAMP032
# MAGIC with history;
# MAGIC
# MAGIC -- Share history for versioning & time-travel - can increases storage and transfer volumes depending on underlying tbl properties
# MAGIC -- Unlocks readStream for recipients
# MAGIC alter share ${share_d2o}
# MAGIC add table ${catalog}.${schema}.campaign_raw
# MAGIC partition (campaign_id = "CAMP032", exposure_year like '2024') as
# MAGIC ${schema}.campaign_kpis_CAMP032_2024
# MAGIC with history;
# MAGIC
# MAGIC alter share ${share_d2d}
# MAGIC add table ${catalog}.${schema}.campaign_kpis
# MAGIC partition (campaign_id = "CAMP001") as 
# MAGIC ${schema}.campaign_kpis_CAMP001;
# MAGIC
# MAGIC -- -- Remove Share
# MAGIC -- -- alter share ${share_d2o} remove table ${catalog}.${schema}.campaign_raw;
# MAGIC alter share ${share_d2d} remove table ${schema}.campaign_kpis_CAMP032;

# COMMAND ----------

# DBTITLE 1,Adding Tables for D2D Share
# MAGIC %sql
# MAGIC -- alter share ${share_d2d}
# MAGIC -- add table ${catalog}.${schema}.ads
# MAGIC -- as ${schema}.ads_d2d;
# MAGIC
# MAGIC -- alter share ${share_d2d}
# MAGIC -- add table ${catalog}.${schema}.campaigns as ${schema}.campaigns_d2d;
# MAGIC
# MAGIC -- alter share ${share_d2d}
# MAGIC -- add table ${catalog}.${schema}.attributions as ${schema}.attributions_d2d;
# MAGIC
# MAGIC -- alter share ${share_d2d}
# MAGIC -- add table ${catalog}.${schema}.conversions as ${schema}.conversions_d2d;
# MAGIC
# MAGIC -- alter share ${share_d2d}
# MAGIC -- add table ${catalog}.${schema}.exposures as ${schema}.exposures_d2d;
# MAGIC
# MAGIC -- alter share ${share_d2d}
# MAGIC -- add table ${catalog}.${schema}.viewers as ${schema}.viewers_d2d;
# MAGIC
# MAGIC -- Alternativley, we can add an entire schema to a share
# MAGIC ALTER SHARE ${share_d2d} ADD SCHEMA ${catalog}.${schema} COMMENT 'Adding the entire schema to the share. Any tables I add will be automatically shared - so watch out!';

# COMMAND ----------

# MAGIC %md
# MAGIC ### Add Notebook to Share
# MAGIC We need to do this step in the UI!!! Click, [here](https://e2-demo-field-eng.cloud.databricks.com/explore/sharing/shares?o=1444828305810485) to navigate to **Catalog** > **Delta Sharing**.

# COMMAND ----------

# DBTITLE 1,Displaying All Items in Specified Share
# MAGIC %sql
# MAGIC show all in share ${share_d2d};

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Create our Recipients(s)

# COMMAND ----------

# DBTITLE 1,Create Recipient
# MAGIC %sql
# MAGIC -- Share to databricks
# MAGIC create recipient campaign_recipient_databricks
# MAGIC using ID 'gcp:us-central1:aa75beb2-fa78-4117-9f14-06a10371db62';
# MAGIC
# MAGIC -- Open sharing, generate credential file
# MAGIC create recipient if not exists campaign_recipient_open;
# MAGIC
# MAGIC -- Add users who can access
# MAGIC ALTER RECIPIENT campaign_recipient_databricks OWNER TO `account users`;
# MAGIC ALTER RECIPIENT campaign_recipient_open OWNER TO `account users`;
# MAGIC
# MAGIC -- -- Delete
# MAGIC -- drop recipient campaign_recipient_databricks;

# COMMAND ----------

# MAGIC %sql
# MAGIC describe recipient campaign_recipient_open;

# COMMAND ----------

# DBTITLE 1,Download Creds
# MAGIC %py
# MAGIC def download_recipient_credential(recipient, location):
# MAGIC   import urllib
# MAGIC   sql(f"""DROP RECIPIENT {recipient}""")
# MAGIC   sql(f"""CREATE RECIPIENT {recipient}""")
# MAGIC   
# MAGIC   df = sql(f"""DESCRIBE RECIPIENT {recipient}""")
# MAGIC   
# MAGIC   if 'info_name' in df.columns:
# MAGIC     link = df.where("info_name = 'activation_link'").collect()[0]['info_value']
# MAGIC   
# MAGIC   else:
# MAGIC     link = df.collect()[0]['activation_link']
# MAGIC   
# MAGIC   if link is not None:
# MAGIC     link = link.replace('delta_sharing/retrieve_config.html?','api/2.0/unity-catalog/public/data_sharing_activation/')
# MAGIC     urllib.request.urlretrieve(link, f"/tmp/{recipient}.share")
# MAGIC     dbutils.fs.mv(f"file:/tmp/{recipient}.share", location)
# MAGIC     print(f"Your file was downloaded to: {location}")

# COMMAND ----------

# DBTITLE 1,Download Cred
download_recipient_credential("campaign_recipient_databricks", "dbfs:/FileStore/campagin_kpi_databricks.share")

# COMMAND ----------

# DBTITLE 1,Grant Privileges
# MAGIC %sql
# MAGIC GRANT select on share ${share_d2o} to recipient campaign_recipient_open;
# MAGIC GRANT select on share ${share_d2d} to recipient campaign_recipient_databricks;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4: Recipient(s) Access
# MAGIC With Open Sharing Recipients, can access their data using Pandas, Spark, PowerBI and more!
# MAGIC
# MAGIC With Databricks Sharing, Recipients can interact with the assets in their shares as they would interact with any asset (notebook, table, view, model) within their workspace.

# COMMAND ----------

# DBTITLE 1,Access Open Share with Pandas
displayHTML(f'''<div style="width:1150px; margin:auto"><iframe src="https://docs.google.com/presentation/d/1FfB9zWLJsBklTLs4iuI3w_B-5CiUz9cAG5da9ItdK4o/embed?slide=1" frameborder="0" width="1150" height="683"></iframe></div>''')