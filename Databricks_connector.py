from databricks import sql
import os

connection = sql.connect(
                        server_hostname = "dbc-6b7ef358-a246.cloud.databricks.com",
                        http_path = "/sql/1.0/warehouses/f9c81dfbc817c079",
                        access_token = "dapib20346b6a3cef452ec9987dc2cd92e34")

cursor = connection.cursor()

cursor.execute("SELECT * from range(10)")
print(cursor.fetchall())

cursor.close()
connection.close()


