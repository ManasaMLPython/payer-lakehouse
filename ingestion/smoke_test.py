from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("smoke_test").getOrCreate()

spark.sql("""
    CREATE TABLE IF NOT EXISTS glue.bronze.smoke_test (id INT, name STRING)
    USING iceberg
""")
spark.sql("INSERT INTO glue.bronze.smoke_test VALUES (1,'a'), (2,'b'), (3,'c')")
spark.sql("SELECT * FROM glue.bronze.smoke_test").show()