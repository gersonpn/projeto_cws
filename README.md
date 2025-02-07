
# Manipulação de DataFrames com PySpark

## Etapas do Processo

1. **Junção de DataFrames:**
   - Realiza uma junção (inner join) entre os DataFrames `df_vendas` e `df_clientes` usando a coluna `cliente_id`.
   - Apenas os registros que possuem correspondência em ambos os DataFrames são mantidos.

2. **Filtragem:**
   - Filtra as vendas, mantendo apenas aquelas cujo valor seja superior a 200.

3. **Conversão de Formato de Data:**
   - Converte a coluna `data_venda` de string para o tipo `Date`, usando o formato `yyyy-MM-dd`.

4. **Seleção e Renomeação de Colunas:**
   - Seleciona as colunas relevantes (`venda_id`, `nome`, `valor`, `data_venda`) e renomeia a coluna `nome` para "nome (nome do cliente)".

5. **Exibição dos Dados:**
   - Exibe o DataFrame resultante no console com o método `.show()`.

## Código

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date

# Realiza a junção entre os DataFrames df_vendas e df_clientes,
df_novo = df_vendas.join(df_clientes, on="cliente_id", how="inner")
# Filtra a linha valor dentro do novo dataframe criado com mais de 200, com o metodo filter
df_novo = df_novo.filter(col("valor") > 200)
#Converte a coluna data_venda para o formato DateType.
df_novo = df_novo.withColumn("data_venda", to_date(col("data_venda"), "yyyy-MM-dd"))
#Seleciona e renomeie as colunas para que o resultado final contenha apenas:
df_novo = df_novo.select(col("venda_id"), col("nome").alias("nome (nome do cliente)"),col("valor"), col("data_venda"))
# Printa o dataframe
df_novo.show()


```

## Como Rodar

1. Instale o PySpark, se necessário:

   ```bash
   pip install pyspark
   ```
___