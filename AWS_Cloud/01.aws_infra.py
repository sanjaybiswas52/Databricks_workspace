# Databricks notebook source
# MAGIC %md
# MAGIC ###🔑 Large‑Scale Migration
# MAGIC 1. AWS <b>DataSync</b> (Online Transfer)
# MAGIC 2. AWS <b>Snowball</b> (Offline Transfer)
# MAGIC
# MAGIC &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;The best approach to migrate very large datasets (like 4+ TB) from on‑premises to Amazon S3 is to use AWS’s dedicated transfer services such as AWS DataSync or AWS Snowball. DataSync is ideal if you have reliable high‑bandwidth connectivity, while Snowball (physical appliance) is recommended when network transfer would be too slow or impractical.
# MAGIC
# MAGIC 1. <b>AWS DataSync (Online Transfer)</b>
# MAGIC <ul><li>What it is: A managed service that automates and accelerates data transfer between on‑premises storage and AWS.
# MAGIC <li><b>Best for:</b> When you have a stable, high‑speed internet or Direct Connect link.
# MAGIC <li>Features:
# MAGIC <ul><li>Encryption in transit and at rest.
# MAGIC <li>End‑to‑end data integrity validation.
# MAGIC <li>Bandwidth throttling and scheduling.
# MAGIC <li>Scales to petabytes of data.</ul>
# MAGIC <li><b>Pros:</b> No hardware needed, continuous sync possible.
# MAGIC <li><b>Cons:</b> Dependent on network bandwidth; may take days/weeks for 4+ TB if bandwidth is limited.</ul>
# MAGIC
# MAGIC 2. <b>AWS Snowball (Offline Transfer)</b>
# MAGIC <ul><li>What it is: A physical appliance shipped to your data center. You load your data locally, then ship it back to AWS where it is ingested into S3.
# MAGIC <li><b>Best for:</b> When network transfer is impractical (e.g., limited bandwidth, unreliable connectivity).
# MAGIC <li>Features:
# MAGIC <ul><li>Each Snowball device can handle 50–80 TB usable capacity.
# MAGIC <li>Data is encrypted with AWS KMS.
# MAGIC <li>Multiple devices can be used for larger datasets.</ul>
# MAGIC <li><b>Pros:</b> Faster than transferring 4+ TB over slow networks.
# MAGIC <li><b>Cons:</b> Requires physical handling and shipping.

# COMMAND ----------

# MAGIC %md
# MAGIC The best approach to migrate very large datasets (like 4+ TB) from on‑premises to Amazon S3 is to use AWS’s dedicated transfer services such as AWS DataSync or AWS Snowball. DataSync is ideal if you have reliable high‑bandwidth connectivity, while Snowball (physical appliance) is recommended when network transfer would be too slow or impractical.