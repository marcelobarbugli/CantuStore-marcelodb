# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, min as spark_min
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# Criação da sessão Spark
spark = SparkSession.builder \
    .appName("Organização Empresarial") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# COMMAND ----------

# Definindo a estrutura usando StructType
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("nome", StringType(), True),
    StructField("salario", IntegerType(), True),
    StructField("lider_id", IntegerType(), True)
])

# COMMAND ----------

data = [
    (40, "Helen", 1500, 50),
    (50, "Bruno", 3000, 10),
    (10, "Leonardo", 4500, 20),
    (20, "Marcos", 10000, None),
    (70, "Mateus", 1500, 10),
    (60, "Cinthia", 2000, 70),
    (30, "Wilian", 1501, 50)
]

df = spark.createDataFrame(data, schema=schema)

# COMMAND ----------

def find_indirect_bosses(df):
    emp = df.alias("emp")
    bosses = emp.select(col("id").alias("empregado_id"), col("lider_id").alias("chefe_indireto_id"))
    
    for _ in range(df.count()):
        next_level = bosses.alias("current") \
            .join(emp.alias("next"), col("current.chefe_indireto_id") == col("next.id"), "left_outer") \
            .select(col("current.empregado_id"), col("next.lider_id").alias("new_chefe_id")) \
            .where(col("new_chefe_id").isNotNull())
        
        bosses = bosses.union(next_level.select(col("empregado_id"), col("new_chefe_id").alias("chefe_indireto_id")))

    return bosses.dropDuplicates()

# Gerar a lista de chefes indiretos
indirect_bosses = find_indirect_bosses(df)

# Filtrar chefes que atendem à condição de salário e encontrar o chefe qualificado de menor ID
result = indirect_bosses.alias("indirect") \
    .join(df.alias("emp"), col("indirect.empregado_id") == col("emp.id")) \
    .join(df.alias("boss"), col("indirect.chefe_indireto_id") == col("boss.id")) \
    .where(col("boss.salario") >= 2 * col("emp.salario")) \
    .groupBy("empregado_id") \
    .agg(spark_min("chefe_indireto_id").alias("chefe_indireto_id")) \
    .select(
        col("empregado_id"),
        col("chefe_indireto_id")
    ) \
    .orderBy("empregado_id")

# Incluindo todos os empregados, mesmo aqueles sem um chefe qualificado
final_result = df.select("id").alias("emp") \
    .join(result.alias("res"), col("emp.id") == col("res.empregado_id"), "left_outer") \
    .select(
        col("emp.id").alias("id_do_funcionário"),
        when(col("res.chefe_indireto_id").isNull(), None).otherwise(col("res.chefe_indireto_id")).alias("id_do_chefe")
    ) \
    .orderBy("id_do_funcionário")

final_result.show()

# COMMAND ----------

# Salvar na camada trusted e mostrar o resultado
final_result.write.format("parquet").mode("overwrite").save("/mnt/delta/trusted/chefes_indiretos")

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS chefes_indiretos;

# COMMAND ----------

# Salvar o DataFrame como uma tabela
final_result.write.saveAsTable("chefes_indiretos")

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL chefes_indiretos;
