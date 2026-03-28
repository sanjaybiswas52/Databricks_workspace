# Databricks notebook source
# MAGIC %md
# MAGIC ###🔹 ICEBERG vs DELTA Tables

# COMMAND ----------

# MAGIC %md
# MAGIC ####1️⃣ High‑level difference (one‑liner)
# MAGIC
# MAGIC <b>Delta Lake</b> is a Databricks‑optimized table format tightly integrated with Spark & Databricks.
# MAGIC <br><b>Apache Iceberg</b> is an open table format designed for multi‑engine, multi‑platform interoperability.
# MAGIC
# MAGIC <b>👉 Think of it like this:</b>
# MAGIC
# MAGIC <b>Delta</b> = Best experience inside Databricks. If only Databricks uses the data → Delta is perfect
# MAGIC <br><b>Iceberg</b> = Best choice across tools (Spark, Trino, Flink, Athena, Snowflake, etc.). If multiple engines must read/write → Iceberg is safer

# COMMAND ----------

# MAGIC %md
# MAGIC ####2️⃣ Metadata & scalability
# MAGIC
# MAGIC <table border="1" cellpadding="8" cellspacing="0" style="border-collapse: collapse; width: 100%;">
# MAGIC   <thead style="background-color: #f2f2f2;">
# MAGIC     <tr>
# MAGIC       <th align="left">Aspect</th>
# MAGIC       <th align="left">Delta Lake</th>
# MAGIC       <th align="left">Apache Iceberg</th>
# MAGIC     </tr>
# MAGIC   </thead>
# MAGIC   <tbody>
# MAGIC     <tr>
# MAGIC       <td>Metadata storage</td>
# MAGIC       <td><code>_delta_log</code> (JSON + checkpoint files)</td>
# MAGIC       <td>Metadata files + manifest lists</td>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC       <td>Handling very large tables</td>
# MAGIC       <td>Good</td>
# MAGIC       <td><strong>Better (designed for PB scale)</strong></td>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC       <td>Partition evolution</td>
# MAGIC       <td>Limited</td>
# MAGIC       <td><strong>Excellent</strong></td>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC       <td>Metadata scan performance</td>
# MAGIC       <td>Can degrade as table grows</td>
# MAGIC       <td><strong>Optimized via manifests</strong></td>
# MAGIC     </tr>
# MAGIC   </tbody>
# MAGIC </table>
# MAGIC
# MAGIC <p>
# MAGIC   <strong>Practical impact:</strong><br/>
# MAGIC   Iceberg’s metadata design avoids full table scans and scales better for 
# MAGIC  very large, long‑running tables, while Delta works well but is more
# MAGIC   optimized for Databricks‑centric workloads.
# MAGIC </p>
# MAGIC <p>Iceberg was designed to <b>avoid</b>:
# MAGIC <ul><li>
# MAGIC Too many small files
# MAGIC <li>Slow metadata scans
# MAGIC <li>Partition rewrites</ul>
# MAGIC </p>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ####3️⃣ important facts
# MAGIC <table border="1" cellpadding="8" cellspacing="0" style="border-collapse: collapse; width: 100%;">
# MAGIC   <thead style="background-color: #f2f2f2;">
# MAGIC     <tr>
# MAGIC       <th align="left">Feature</th>
# MAGIC       <th align="left">Delta</th>
# MAGIC       <th align="left">Iceberg</th>
# MAGIC     </tr>
# MAGIC   </thead>
# MAGIC   <tbody>
# MAGIC     <tr>
# MAGIC       <td>Time travel</td>
# MAGIC       <td>Excellent</td>
# MAGIC       <td>Excellent</td>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC       <td>Row‑level deletes</td>
# MAGIC       <td>(MERGE, DELETE)</td>
# MAGIC       <td>(native row‑level deletes)</td>
# MAGIC     </tr>
# MAGIC      <tr>
# MAGIC       <td>Partition</td>
# MAGIC       <td>Partition is physical- "Changing partition will rewrite data"<br>
# MAGIC CREATE TABLE sales_delta
# MAGIC PARTITIONED BY (country);
# MAGIC </br></td>
# MAGIC       <td><strong><strong>Partition is logical-</strong> evolve without rewriting data<br><b>
# MAGIC ALTER TABLE sales_iceberg
# MAGIC SET PARTITION SPEC (years(order_date), country);
# MAGIC </b></br></strong></td>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC       <td>Partition evolution</td>
# MAGIC       <td> (painful) Changing partition = rewrite data</td>
# MAGIC       <td><strong><strong>(easy)</strong> (evolve it without rewriting data)</strong></td>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC       <td>Long‑term data lake (10+ years)</td>
# MAGIC       <td>100% Databricks ecosystem</td>
# MAGIC       <td><strong>Iceberg is safer because it is engine‑agnostic</strong></td>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC       <td>Table</td>
# MAGIC       <td>
# MAGIC CREATE TABLE orders_delta
# MAGIC <br>USING DELTA
# MAGIC <br>AS SELECT * FROM raw_orders;
# MAGIC </td>
# MAGIC       <td>
# MAGIC CREATE TABLE orders_iceberg
# MAGIC <br>USING ICEBERG
# MAGIC <br>AS SELECT * FROM raw_orders;
# MAGIC </td>
# MAGIC     </tr>
# MAGIC   </tbody>
# MAGIC </table>

# COMMAND ----------

# MAGIC %md
# MAGIC ###🔹 Best Practices for Better Clusters

# COMMAND ----------

# MAGIC %md
# MAGIC ####1. Cluster Sizing
# MAGIC <ul><li><b>Start small, scale dynamically:</b> Use autoscaling clusters so Spark jobs can expand resources during heavy loads and shrink when idle.
# MAGIC <li><b>Choose instance types wisely:</b>
# MAGIC <ul><li><b>Memory‑optimized nodes</b> → best for joins, aggregations, and caching large DataFrames.
# MAGIC <li><b>Compute‑optimized nodes</b> → best for ETL transformations and CPU‑heavy workloads.</ul></ul>

# COMMAND ----------

# MAGIC %md
# MAGIC ####2. Cluster Configuration
# MAGIC <ul><li><b>Enable Adaptive Query Execution (AQE)</b> → Spark automatically adjusts shuffle partitions and optimizes skew handling.
# MAGIC <li><b>Photon Engine (Databricks runtime)</b> → accelerates SQL and Delta Lake queries with vectorized execution.
# MAGIC <li><b>Use Delta Lake</b> → supports data skipping, Z‑ordering, and efficient caching.

# COMMAND ----------

# MAGIC %md
# MAGIC ####3. Data Layout & Partitioning
# MAGIC <ul><li><b>Partition wisely:</b> Avoid too many small files; aim for 128 MB – 1 GB per file in S3.
# MAGIC <li><b>Z‑ordering:</b> Organize data by frequently queried columns to reduce scan time.
# MAGIC <li><b>Optimize writes:</b> Use OPTIMIZE and VACUUM commands in Delta Lake to compact small files.

# COMMAND ----------

# MAGIC %md
# MAGIC ####4. Caching & Storage
# MAGIC <ul><li><b>Cache intermediate results</b> in memory when reused multiple times.
# MAGIC <li><b>Use SSD‑backed storage</b> for faster shuffle operations.
# MAGIC <li><b>Leverage S3 with ORC/Parquet + compression (Snappy/ZSTD)</b> for efficient storage and retrieval.

# COMMAND ----------

# MAGIC %md
# MAGIC ####5. Monitoring & Debugging
# MAGIC <ul><li><b>Spark UI:</b> Analyze stages, tasks, and shuffle operations to detect bottlenecks.
# MAGIC <li><b>Ganglia/Databricks metrics:</b> Monitor CPU, memory, and I/O usage.
# MAGIC <li><b>Breakpoints & logging:</b> Debug transformations before scaling to full datasets.

# COMMAND ----------

# MAGIC %md
# MAGIC ####📊 Comparison Table: Cluster Choices
# MAGIC <table border="1" cellpadding="6" cellspacing="0">
# MAGIC   <thead>
# MAGIC     <tr>
# MAGIC       <th>Feature</th>
# MAGIC       <th>Small Cluster</th>
# MAGIC       <th>Autoscaling Cluster</th>
# MAGIC       <th>Photon Runtime</th>
# MAGIC     </tr>
# MAGIC   </thead>
# MAGIC   <tbody>
# MAGIC     <tr>
# MAGIC       <td>Cost Efficiency</td>
# MAGIC       <td>Low</td>
# MAGIC       <td>High</td>
# MAGIC       <td>High</td>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC       <td>Performance</td>
# MAGIC       <td>Limited</td>
# MAGIC       <td>Scales dynamically</td>
# MAGIC       <td>Very high</td>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC       <td>Best Use Case</td>
# MAGIC       <td>Dev/testing</td>
# MAGIC       <td>Production ETL</td>
# MAGIC       <td>SQL/BI queries</td>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC       <td>Handles Skew</td>
# MAGIC       <td>No</td>
# MAGIC       <td>Yes (with AQE)</td>
# MAGIC       <td>Yes</td>
# MAGIC     </tr>
# MAGIC   </tbody>
# MAGIC </table>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ###🔹 Databricks Clean Rooms (as concept)
# MAGIC &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Databricks Clean Rooms are secure, privacy‑centric collaboration environments that let multiple organizations analyze shared data without exposing raw datasets. They work by using Delta Sharing and Unity Catalog to enforce a “no‑trust” model, so each party can run queries or ML workloads on shared data while keeping their own data private.
# MAGIC
# MAGIC <br><b>🔑 How Databricks Clean Rooms Work</b>
# MAGIC <ul><li><b>Privacy by design:</b> Parties never see each other’s raw data; they only see aggregated or query results.
# MAGIC <li><b>Delta Sharing:</b> Data stays in place (e.g., in S3, ADLS, or other cloud storage) and is shared securely via Delta Sharing.
# MAGIC <li><b>Unity Catalog:</b> Provides governance, access control, and auditing for all shared assets.
# MAGIC <li><b>Serverless compute:</b> Queries run in isolated environments, ensuring no data leakage.
# MAGIC <li><b>Multi‑party collaboration:</b> Up to 10 collaborators can join a clean room, each contributing datasets or notebooks.
# MAGIC
# MAGIC <b>📊 Example Scenario</b>
# MAGIC <br>Imagine two companies:
# MAGIC <ul><li>Retailer A has customer purchase data.
# MAGIC <li>Brand B has product campaign data.</li></ul>
# MAGIC They want to analyze campaign effectiveness without exposing sensitive customer details.
# MAGIC
# MAGIC <b>Steps:</b>
# MAGIC <ol><li><b>Retailer A</b> creates a clean room in Databricks.
# MAGIC <li>They add their purchase dataset (stored in Delta Lake).
# MAGIC <li><b>Brand B</b> joins the clean room and adds campaign data.
# MAGIC <li>The results show aggregated insights (campaign vs purchases), but neither party sees the other’s raw data.
# MAGIC <li>Both parties run queries like:
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT campaign_id, COUNT(*) AS purchases
# MAGIC FROM retailer_purchases p
# MAGIC JOIN brand_campaigns c
# MAGIC ON p.product_id = c.product_id
# MAGIC GROUP BY campaign_id;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC <b>✅ Benefits</b>
# MAGIC <ul><li><b>Data privacy:</b> Sensitive data never leaves its owner’s environment.
# MAGIC <li><b>Cross‑cloud collaboration:</b> Works across AWS, Azure, GCP without moving data.
# MAGIC <li><b>Flexible workloads:</b> Supports SQL, Python, R, Scala, and ML libraries.
# MAGIC <li><b>Governance:</b> Unity Catalog ensures compliance and auditability.