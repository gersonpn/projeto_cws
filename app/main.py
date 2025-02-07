# Importa as bibliotecas necessárias
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date

#inicia a sessão spark
spark = SparkSession.builder.appName("teste_cws").getOrCreate()

#Cria os dois dataframes
df_vendas = spark.createDataFrame([
    (1, 101, 100.50, "2024-01-10"),
    (2, 102, 250.75, "2024-01-12"),
    (3, 101, 300.00, "2024-01-15"),
    (4, 103, 150.25, "2024-01-20"),
], ["venda_id", "cliente_id", "valor", "data_venda"])

df_clientes = spark.createDataFrame([
    (101, "João", "1990-05-15"),
    (102, "Maria", "1985-08-20"),
    (103, "Pedro", "1998-02-25"),
    (104, "Carla", "2000-11-10"),
], ["cliente_id", "nome", "data_nascimento"])

# Realize a junção entre os DataFrames df_vendas e df_clientes,
df_novo = df_vendas.join(df_clientes, on="cliente_id", how="inner")
# Filtra a linha valor dentro do novo dataframe criado com mais de 200, com o metodo filter
df_novo = df_novo.filter(col("valor") > 200)
#Converta a coluna data_venda para o formato DateType.
df_novo = df_novo.withColumn("data_venda", to_date(col("data_venda"), "yyyy-MM-dd"))
#Selecione e renomeie as colunas para que o resultado final contenha apenas:
df_novo = df_novo.select(col("venda_id"), col("nome").alias("nome (nome do cliente)"),col("valor"), col("data_venda"))
# Printa o dataframe
df_novo.show()

