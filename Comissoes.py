# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date
from pyspark.sql.types import StructType, StructField, StringType, DateType, FloatType

# Criação da sessão Spark
spark = SparkSession.builder \
    .appName("Comissão") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# COMMAND ----------

# Definindo o esquema da tabela
schema = StructType([
    StructField("comprador", StringType(), True),
    StructField("vendedor", StringType(), True),
    StructField("dataPgto", StringType(), True),  # Usando StringType temporariamente
    StructField("valor", FloatType(), True)
])

# COMMAND ----------

data = [
    ("Leonardo", "Bruno", "2000-01-01", 200.0),
    ("Leonardo", "Matheus", "2003-09-27", 1024.0),
    ("Leonardo", "Lucas", "2006-06-26", 512.0),
    ("Marcos", "Lucas", "2020-12-17", 100.0),
    ("Marcos", "Lucas", "2002-03-22", 10.0),
    ("Cinthia", "Lucas", "2021-03-20", 500.0),
    ("Mateus", "Bruno", "2007-06-02", 400.0),
    ("Mateus", "Bruno", "2006-06-26", 400.0),
    ("Mateus", "Bruno", "2015-06-26", 200.0)
]

# COMMAND ----------

# Criando um DataFrame e registrando como uma tabela temporária
df = spark.createDataFrame(data, schema=schema)
# Convertendo a coluna 'dataPgto' para DateType
df = df.withColumn("dataPgto", to_date(col("dataPgto"), "yyyy-MM-dd"))

# Registrando a tabela temporária
df.createOrReplaceTempView("comissoes")

# COMMAND ----------

# Query para selecionar os vendedores conforme o critério especificado
query = """
SELECT vendedor
FROM (
    SELECT vendedor, valor,
           SUM(valor) OVER (PARTITION BY vendedor ORDER BY dataPgto ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as running_total
    FROM comissoes
) AS comissions
WHERE running_total >= 1024
GROUP BY vendedor
HAVING COUNT(*) <= 3
ORDER BY vendedor ASC
"""

result = spark.sql(query)
result.show()

# COMMAND ----------

# Salvando o resultado na camada Gold
result.write.format("parquet").mode("overwrite").save("/mnt/delta/gold/comissoes_gold")

# COMMAND ----------


