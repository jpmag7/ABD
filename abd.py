from pyspark.sql import SparkSession
import os

folder_path = '/app/tabelas'

spark = SparkSession.builder.master("spark://spark:7077").getOrCreate()

for filename in os.listdir(folder_path):
    file_path = os.path.join(folder_path, filename)
    
    if filename.endswith('.parquet'):
        table_df = spark.read.parquet(file_path)
        table_df.createOrReplaceTempView(filename.replace(".parquet", ""))

# Query 1
resultado1 = spark.sql("""
    SELECT *
    FROM table1
    JOIN table2 ON table1.id = table2.id
    JOIN table3 ON table1.id = table3.id
    WHERE table1.column = 'value'
""")

resultado1.show()