from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import DateType

# Criando a Spark Session
spark = SparkSession.builder.appName("VendasClientes").getOrCreate()

# Criando os DataFrames
vendas_data = [(1, 101, 100.50, "2024-01-10"),
               (2, 102, 250.75, "2024-01-12"),
               (3, 101, 300.00, "2024-01-15"),
               (4, 103, 150.25, "2024-01-20")]

clientes_data = [(101, "João", "1990-05-15"),
                 (102, "Maria", "1985-08-20"),
                 (103, "Pedro", "1998-02-25"),
                 (104, "Carla", "2000-11-10")]