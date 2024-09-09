# Databricks notebook source
# MAGIC %md
# MAGIC ### Analysis_Abandoned_Carts - Parte 2

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, month, when, current_timestamp, collect_set, explode, size
from pyspark.sql.types import IntegerType, DoubleType, ArrayType, StructType, StructField, LongType
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Iniciando a sessão Spark
spark = SparkSession.builder.appName("Analysis of Abandoned Carts").getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### Load CSV and Parquet files

# COMMAND ----------

# Carregar dados
df_regions = spark.read.csv("/FileStore/tb_regions.csv", header=True, inferSchema=True, sep="|")
df_cmssitelp = spark.read.csv("/FileStore/tb_cmssitelp.csv", header=True, inferSchema=True, sep="|")
df_users = spark.read.csv("/FileStore/tb_users.csv", header=True, inferSchema=True, sep="|")
df_paymentmodes = spark.read.csv("/FileStore/tb_paymentmodes.csv", header=True, inferSchema=True, sep="|")
df_paymentinfos = spark.read.parquet("/FileStore/tb_paymentinfos/tb_paymentinfos.parquet")
df_carts = spark.read.parquet("/FileStore/tb_carts/tb_carts-1.parquet")
df_cart_entries = spark.read.parquet("/FileStore/tb_cartentries/tb_cartentries.parquet")
df_addresses = spark.read.parquet("/FileStore/tb_addresses/tb_addresses.parquet")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### Create schema and Data Cleaning

# COMMAND ----------

# Criar o schema 'silver' se não existir
spark.sql("CREATE SCHEMA IF NOT EXISTS silver")

# Definição de funções de limpeza e validação
def clean_and_validate_df(df, primary_key, timestamp_cols):
    # Limpeza de nulos em colunas essenciais
    df = df.filter(col(primary_key).isNotNull())
    for timestamp_col in timestamp_cols:
        df = df.filter(col(timestamp_col).isNotNull())
    
    # Deduplicação baseada na chave primária
    df = df.dropDuplicates([primary_key])
    
    return df

# Limpeza e validação de cada DataFrame
df_regions = clean_and_validate_df(df_regions, "PK", ["createdTS", "modifiedTS"])
df_cmssitelp = clean_and_validate_df(df_cmssitelp, "ITEMPK", [])
df_users = clean_and_validate_df(df_users, "PK", ["createdTS", "modifiedTS"])
df_paymentmodes = clean_and_validate_df(df_paymentmodes, "PK", ["createdTS", "modifiedTS"])
df_paymentinfos = clean_and_validate_df(df_paymentinfos, "PK", [])
df_carts = clean_and_validate_df(df_carts, "PK", ["createdTS", "modifiedTS"])
df_cart_entries = clean_and_validate_df(df_cart_entries, "PK", ["createdTS", "modifiedTS"])
df_addresses = clean_and_validate_df(df_addresses, "PK", ["createdTS", "modifiedTS"])

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### Auditing Columns

# COMMAND ----------

# Função para adicionar colunas de auditoria
def add_audit_columns(df):
    return df.withColumn("load_timestamp", current_timestamp())

# Adicionar colunas de auditoria
df_regions = add_audit_columns(df_regions)
df_cmssitelp = add_audit_columns(df_cmssitelp)
df_users = add_audit_columns(df_users)
df_paymentmodes = add_audit_columns(df_paymentmodes)
df_paymentinfos = add_audit_columns(df_paymentinfos)
df_carts = add_audit_columns(df_carts)
df_cart_entries = add_audit_columns(df_cart_entries)
df_addresses = add_audit_columns(df_addresses)

# COMMAND ----------

# DBTITLE 1,Quais os produtos que mais tiveram carrinhos abandonados?
# DataFrame que contém informações sobre os carrinhos e se foram abandonados
abandoned_carts = df_cart_entries.filter(F.col("p_totalprice") == 0)
abandoned_carts = abandoned_carts.withColumn("p_product", col("p_product").cast("long"))
abandoned_carts = abandoned_carts.withColumn("p_quantity", col("p_quantity").cast("long"))

# Agrupar por identificador de produto e somar as quantidades
abandoned_carts = abandoned_carts.groupBy("p_product").agg(F.sum("p_quantity").alias("total_quantity"))

# Ordenar os produtos por total_quantity de forma descendente
most_abandoned_products = abandoned_carts.orderBy(F.col("total_quantity").desc())

most_abandoned_products.show()

# COMMAND ----------

# DBTITLE 1,Quais as duplas de produtos em conjunto que mais tiveram carrinhos abandonados?
# NÃO CONSEGUI RESPONDER ESSA PERGUNTA
# Definindo uma UDF para criar combinações de pares de produtos
def combinations_udf(products_list):
    if len(products_list) > 1:
        return list(itertools.combinations(sorted(products_list), 2))
    else:
        return []

# Registrando a UDF com o tipo correto
combinations_udf = udf(combinations_udf, ArrayType(StructType([
    StructField("item1", LongType(), True),
    StructField("item2", LongType(), True)
])))

# Agrupar por identificador de carrinho e coletar produtos em uma lista
cart_product_pairs = abandoned_carts.groupBy("p_product").agg(collect_set("p_product").alias("products"))

# Aplicar a UDF para criar combinações de produtos
product_pairs = cart_product_pairs.withColumn("product_pair", explode(combinations_udf("products")))

# Explodir a estrutura para acessar os itens individuais das combinações
product_pairs = product_pairs.select(
    col("product_pair.item1").alias("product1"),
    col("product_pair.item2").alias("product2")
)

# Agrupar por pares de produtos e contar
product_pair_counts = product_pairs.groupBy("product1", "product2").count().orderBy(col("count").desc())

product_pair_counts.show()


# COMMAND ----------

# DBTITLE 1,- Quais produtos tiveram um aumento de abandono?
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Converter a coluna de timestamp correta para 'df_cart_entries'
# df_cart_entries = df_cart_entries.withColumn("createdTS", F.to_timestamp(df_cart_entries.p_createdTS))

# Renomear colunas duplicadas no df_cart_entries
for col in ['createdTS', 'modifiedTS', 'PK', 'TypePkString', 'OwnerPkString']:
    df_cart_entries = df_cart_entries.withColumnRenamed(col, 'entries_' + col)

# Fazer o join entre os carrinhos e as entradas de carrinho
df_joined = df_carts.join(df_cart_entries, df_carts.PK == df_cart_entries.p_order, "inner")

# Extrair o mês e o ano da data de criação
df_abandoned_carts = df_joined.withColumn("month_year", F.date_format("createdTS", "yyyy-MM"))

# Agrupar por produto e mês/ano, e contar o número de abandonos
abandon_counts_by_month = df_abandoned_carts.groupBy(df_cart_entries.p_product, "month_year").count()

# Criar uma janela de partição por produto e ordenar por mês/ano
window_spec = Window.partitionBy(df_cart_entries.p_product).orderBy("month_year")

# Calcular o aumento mês a mês no número de abandonos
abandon_counts_by_month = abandon_counts_by_month.withColumn("prev_count", F.lag("count").over(window_spec))
abandon_counts_by_month = abandon_counts_by_month.withColumn("prev_count", F.coalesce(F.col("prev_count"), F.lit(0)))
abandon_counts_by_month = abandon_counts_by_month.withColumn("increase", (F.col("count") - F.col("prev_count")))

# Filtrar apenas os meses onde houve um aumento nos abandonos
increased_abandonments = abandon_counts_by_month.filter(F.col("increase") > 0)

# Selecionar os meses com aumento, ordenar e mostrar os resultados
increased_abandonments.select(df_cart_entries.p_product.alias("Product"), "month_year", "increase").orderBy("Product", "month_year").show()


# COMMAND ----------

# DBTITLE 1,Quais os produtos novos e a quantidade de carrinhos no seu primeiro mês de lançamento?
# Converter a coluna 'entries_createdTS' para timestamp
df_cart_entries = df_cart_entries.withColumn("entries_createdTS", F.to_timestamp(df_cart_entries.entries_createdTS))

# Identificar o primeiro mês de aparição de cada produto
window_spec = Window.partitionBy("p_product")
first_appearance = df_cart_entries.withColumn("first_month", F.min("entries_createdTS").over(window_spec))

# Formatar o primeiro mês para "yyyy-MM"
first_appearance = first_appearance.withColumn("first_month", F.date_format("first_month", "yyyy-MM"))

# Garantir que p_product seja um inteiro
first_appearance = first_appearance.withColumn("p_product", first_appearance["p_product"].cast(LongType()))

# Filtrar os dados para incluir apenas entradas no primeiro mês de lançamento de cada produto e excluir entradas onde p_product seja null
df_first_month = first_appearance.filter((F.col("entries_createdTS").substr(1, 7) == F.col("first_month")) & (F.col("p_product").isNotNull()))

# Agrupar por produto e contar o número de carrinhos abandonados
abandon_count_first_month = df_first_month.groupBy("p_product").count()

# Ordenar e mostrar os resultados do maior para o menor
abandon_count_first_month.select("p_product", "count").orderBy(F.col("count").desc()).show()


# COMMAND ----------

# DBTITLE 1,Quais estados tiveram mais abandonos?
# Converter a coluna 'createdTS' para o formato timestamp nos DataFrames necessários
df_carts = df_carts.withColumn("createdTS", F.to_timestamp(df_carts.createdTS))
df_addresses = df_addresses.withColumn("createdTS", F.to_timestamp(df_addresses.createdTS))

# Renomear colunas no df_carts para evitar conflitos no join
df_carts = df_carts.withColumnRenamed("PK", "cart_PK")
df_carts = df_carts.withColumnRenamed("p_deliveryaddress", "delivery_address_PK")

# Fazer o join entre os carrinhos e os endereços
df_cart_address = df_carts.join(df_addresses, df_carts.delivery_address_PK == df_addresses.PK, "inner")

# Fazer o join entre o resultado e as regiões (supondo que p_region corresponda a PK em df_regions)
df_final = df_cart_address.join(df_regions, df_cart_address.p_region == df_regions.PK, "inner")

# Agrupar por estado (supondo que p_isocodeshort representa o código do estado) e contar abandonos
abandon_count_by_state = df_final.groupBy(df_regions.p_isocodeshort).count()

# Ordenar os resultados do maior para o menor
abandon_count_by_state.orderBy(F.col("count").desc()).show()


# COMMAND ----------

# DBTITLE 1,relatorio1
# Conversão de dados e renomeações previamente explicadas
df_carts = df_carts.withColumn("createdTS", F.to_timestamp("createdTS"))
df_carts = df_carts.withColumnRenamed("p_totalprice", "cart_p_totalprice")
df_cart_entries = df_cart_entries.withColumn("entries_createdTS", F.to_timestamp("entries_createdTS"))

# Realizar a junção correta
joined_df = df_carts.join(df_cart_entries, df_carts.cart_PK == df_cart_entries.p_order)

# Agrupamento, agregação e aplicação de formatos específicos
grouped_df = joined_df.groupBy(
    F.month("createdTS").alias("month"),
    df_cart_entries.p_product.alias("product_id")  # Usando alias para clareza
).agg(
    F.countDistinct("cart_PK").alias("abandoned_carts"),
    F.sum("p_quantity").cast(IntegerType()).alias("items_abandoned"),
    F.round(F.sum("p_totalprice"), 2).alias("unbilled_value")  # Round aqui já ajusta para dois decimais
)

# Formatando unbilled_value para mostrar sempre dois decimais
grouped_df = grouped_df.withColumn("unbilled_value", F.format_number("unbilled_value", 2))

# Removendo linhas com qualquer valor null
grouped_df = grouped_df.dropna()

grouped_df.show()


# COMMAND ----------

# DBTITLE 1,relatório2
# Relatório diário
daily_report = df_carts.join(df_cart_entries, df_carts.cart_PK == df_cart_entries.p_order)

# Agrupar por data
daily_grouped = daily_report.groupBy(F.to_date("createdTS").alias("date")) \
    .agg(
        F.countDistinct("cart_PK").alias("abandoned_carts"),
        F.sum("p_quantity").cast(IntegerType()).alias("items_abandoned"),
        F.sum("p_totalprice").alias("unbilled_value")
    )

daily_grouped = daily_grouped.withColumn("unbilled_value", F.format_number("unbilled_value", 2))
daily_grouped.show()


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### Loading data to TXT

# COMMAND ----------

# Renomear 'createdTS' em cada DataFrame antes de fazer join para evitar ambiguidade
df_carts = df_carts.withColumnRenamed("createdTS", "carts_createdTS")
df_users = df_users.withColumnRenamed("createdTS", "users_createdTS")
df_paymentmodes = df_paymentmodes.withColumnRenamed("createdTS", "paymentmodes_createdTS")
df_paymentinfos = df_paymentinfos.withColumnRenamed("createdTS", "paymentinfos_createdTS")
df_addresses = df_addresses.withColumnRenamed("createdTS", "addresses_createdTS")
df_cart_entries = df_cart_entries.withColumnRenamed("createdTS", "cartentries_createdTS")
df_cmssitelp = df_cmssitelp.withColumnRenamed("createdTS", "cmssitelp_createdTS")
df_paymentmodes = df_paymentmodes.withColumnRenamed("p_code", "paymentmodes_p_code")
df_cart_entries = df_cart_entries.withColumnRenamed("p_code", "cartentries_p_code")
df_paymentmodes = df_paymentmodes.withColumnRenamed("p_name", "paymentmodes_p_name")
df_cmssitelp = df_cmssitelp.withColumnRenamed("p_name", "cmssitelp_p_name")
df_cart_entries = df_cart_entries.withColumnRenamed("total_quantity", "cartentries_total_quantity")
df_cmssitelp = df_cmssitelp.withColumnRenamed("total_quantity", "cmssitelp_total_quantity")


# Realizar os joins
export_data = df_carts \
    .join(df_users, df_carts.cart_PK == df_users.PK, "left") \
    .join(df_paymentmodes, df_carts.cart_PK == df_paymentmodes.PK, "left") \
    .join(df_paymentinfos, df_carts.cart_PK == df_paymentinfos.PK, "left") \
    .join(df_addresses, df_carts.cart_PK == df_addresses.PK, "left") \
    .join(df_cart_entries, df_carts.cart_PK == df_cart_entries.p_order, "left") \
    .join(df_cmssitelp, df_cart_entries.p_product == df_cmssitelp.ITEMPK, "left")

# Selecionar os dados necessários e ordenar pelo valor total do carrinho
selected_data = export_data.select(
    "cart_PK", 
    "carts_createdTS",
    "p_totalprice", 
    "p_uid", 
    "paymentmodes_p_code", 
    "p_installments", 
    "cmssitelp_p_name", 
    "p_postalcode", 
    "p_quantity"
).orderBy(F.col("p_totalprice").desc()).limit(50)

# Exibir o resultado
selected_data.show()

# COMMAND ----------

# Assuming aggregation needs to be corrected and properly joined:
cart_entries_agg = df_cart_entries.groupBy("p_order").agg(
    F.sum("p_quantity").alias("total_quantity"),
    F.count("entries_PK").alias("count_entries"),  # Assuming PK is the primary key for cart entries
    F.collect_list(F.struct("p_product", "p_quantity", "p_totalprice")).alias("entries")  # Collects into an array of structs
)

# Joining this back with the main export data:
export_data = df_carts.join(cart_entries_agg, df_carts.cart_PK == cart_entries_agg.p_order, "left")

# COMMAND ----------

selected_data = export_data.select(
    F.concat_ws("|",
        F.col("cart_PK").cast("string"),
        F.date_format("carts_createdTS", "yyyy-MM-dd HH:mm:ss"),
        F.col("cart_p_totalprice").cast("string"),
        "p_guid",
        "p_paymentmode",
        F.col("p_statusinfo").cast("string"),
        "p_name",
        "p_code",
        F.col("total_quantity").cast("string"),  # Ensure this field is derived from the aggregation
        F.col("count_entries").cast("string")  # Ensure this field is derived from the aggregation
    ).alias("line1"),
    F.concat_ws("\n",
        F.transform(
            "entries",  # This should be the correct array column from aggregation
            lambda e: F.concat_ws("|", e["p_product"], e["p_quantity"], e["p_totalprice"])
        )
    ).alias("products_details")
).orderBy(F.col("products_details").desc()).limit(50)


# COMMAND ----------

# Convertendo o DataFrame para uma lista de strings
formatted_data = selected_data.rdd.map(lambda row: f"{row.line1}\n{row.products_details}").collect()

# Escrevendo para um arquivo localmente no driver
file_path = "/tmp/exported_carts.txt"
with open(file_path, "w") as file:
    for line in formatted_data:
        file.write(line + "\n")

# Movendo o arquivo do local temporário para o DBFS
dbutils.fs.mv("file:/tmp/exported_carts.txt", "dbfs:/FileStore/exported_carts.txt")


# COMMAND ----------

# MAGIC %fs head dbfs:/FileStore/exported_carts.txt
