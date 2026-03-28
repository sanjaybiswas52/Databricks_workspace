# Databricks notebook source


# COMMAND ----------

# MAGIC %md
# MAGIC # CDC Pipeline Demo Installation
# MAGIC
# MAGIC This notebook installs the CDC Pipeline demo dataset and assets. Run the cell below to get started.
# MAGIC
# MAGIC **Tip:** Press `Cmd + Enter` (Mac) or `Ctrl + Enter` (Windows/Linux) to run a cell.

# COMMAND ----------

# MAGIC %pip install dbdemos -U
# MAGIC import dbdemos
# MAGIC dbdemos.install('declarative-pipeline-cdc')