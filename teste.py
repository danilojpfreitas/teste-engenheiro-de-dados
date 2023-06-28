import pandas as pd
import findspark
from pyspark.sql import SparkSession

findspark.init()

spark = SparkSession \
        .builder \
        .appName('PySpark MySQL Connection') \
        .master('local[*]') \
        .config("spark.jars", "/workspace/teste-engenheiro-de-dados/jars/mysql-connector-j-8.0.33.jar") \
        .getOrCreate()

spark

etniaColumns = ["codEtnia", "nomeEtnia"]
etniaNome = [(0, "Nao declarado"), (1, "Branca"), (2, "Preta"), (3, "Parda"), (4, "Amarela"), (5, "Indigena")]
etniaDataframe = spark.createDataFrame(etniaNome, etniaColumns)

etniaDataframe.write\
  .format("jdbc")\
  .option("driver","com.mysql.cj.jdbc.Driver")\
  .option("url", "jdbc:mysql://172.18.0.2:3306/mesha")\
  .option("dbtable", "mesha")\
  .option("user", "mesha")\
  .option("password", "mesha")\
  .save()