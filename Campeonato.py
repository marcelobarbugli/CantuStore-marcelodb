# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, sum
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# Criação da sessão Spark
spark = SparkSession.builder \
    .appName("Campeonato") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# COMMAND ----------

# Definir o esquema da tabela
schema = StructType([
    StructField("Time_id", IntegerType(), nullable=False),
    StructField("Time_nome", StringType(), nullable=False)
])

# Dados para a tabela
data = [(10, 'Financeiro'),
        (20, 'Marketing'),
        (30, 'Logística'),
        (40, 'TI'),
        (50, 'Dados')]

# Criar DataFrame
df = spark.createDataFrame(data, schema)

# Salvar como Tabela Delta
df.write.format("delta").mode("overwrite").saveAsTable("times")

# COMMAND ----------

# Definição do schema
schema = StructType([
    StructField("Jogo_id", IntegerType(), nullable=False),
    StructField("Time_mandante", IntegerType(), nullable=False),
    StructField("Time_visitante", IntegerType(), nullable=False),
    StructField("Gols_mandante", IntegerType(), nullable=False),
    StructField("Gols_Visitante", IntegerType(), nullable=False)
])

# Criação de um DataFrame de exemplo
data = [
    (1, 30, 20, 1, 0),
    (2, 10, 20, 1, 2),
    (3, 20, 50, 2, 2),
    (4, 10, 30, 1, 0),
    (5, 30, 50, 0, 1)
]

# Caminho para salvar a Delta Table
delta_table_path = "/mnt/delta/CAMPEONATOS_JOGOS"

jogos_df = spark.createDataFrame(data=jogos_data, schema=schema)

# Salvando como Delta Table
jogos_df.write.format("delta").mode("overwrite").save(delta_table_path)

# Criando a tabela Delta
spark.sql(f"""
CREATE TABLE IF NOT EXISTS jogos
USING DELTA
LOCATION '{delta_table_path}'
""")

# Adicionando a constraint de chave única para jogo_id
# Função para verificar unicidade
def insert_unique_records(new_df: jogos_df, table_name: str, key_column: str):
    existing_ids = spark.sql(f"SELECT {key_column} FROM {table_name}").rdd.flatMap(lambda x: x).collect()
    new_df_filtered = new_df.filter(~new_df[key_column].isin(existing_ids))
    
    new_df_filtered.write.format("delta").mode("append").saveAsTable(table_name)


# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL jogos;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM jogos;

# COMMAND ----------

display(jogos_df)

# COMMAND ----------

# Calculando os pontos de cada jogo
resultado = jogos_df.withColumn("pontos_mandante",
                                when(col("Gols_mandante") > col("Gols_Visitante"), 3)
                                .when(col("Gols_mandante") == col("Gols_Visitante"), 1)
                                .otherwise(0)) \
                    .withColumn("pontos_visitante",
                                when(col("Gols_mandante") < col("Gols_Visitante"), 3)
                                .when(col("Gols_mandante") == col("Gols_Visitante"), 1)
                                .otherwise(0))

# COMMAND ----------

# Agregando os pontos por time
pontos_df = resultado.groupBy("Time_mandante").agg(sum("pontos_mandante").alias("pontos")) \
                    .union(resultado.groupBy("Time_visitante").agg(sum("pontos_visitante").alias("pontos"))) \
                    .groupBy("Time_mandante").agg(sum("pontos").alias("num_pontos")) \
                    .withColumnRenamed("Time_mandante", "Time_id")

# COMMAND ----------

# Juntando com o DataFrame de times para adicionar o nome
resultado_final = pontos_df.join(times_df, "Time_id") \
                           .select("Time_id", "Time_nome", "num_pontos") \
                           .orderBy(col("num_pontos").desc(), "Time_id")

# COMMAND ----------

# Mostrando o resultado
resultado_final.show()


# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE tabela_campeonato (
# MAGIC     Time_id INT,
# MAGIC     Time_nome STRING,
# MAGIC     num_pontos INT
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# Definir o caminho da camada gold
gold_path = "/mnt/delta/gold/tabela_campeonato"  # Altere o caminho conforme necessário

# Salvar o DataFrame como tabela Delta na camada gold
resultado_final.write.format("delta").mode("overwrite").save(gold_path)


# COMMAND ----------

display(resultado_final)

