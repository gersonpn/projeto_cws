# Manipulação de DataFrames com PySpark

Este script realiza operações de transformação e filtragem em DataFrames usando a biblioteca PySpark. O objetivo é unir dados de vendas e clientes, filtrar vendas com valores acima de 200, converter datas e preparar um DataFrame para visualização.

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
from pyspark.sql.functions import col, to_date

# Junção dos DataFrames
df_novo = df_vendas.join(df_clientes, on="cliente_id", how="inner")

# Filtragem das vendas com valor acima de 200
df_novo = df_novo.filter(col("valor") > 200)

# Conversão da coluna 'data_venda' para o formato DateType
df_novo = df_novo.withColumn("data_venda", to_date(col("data_venda"), "yyyy-MM-dd"))

# Seleção e renomeação das colunas
df_novo = df_novo.select(
    col("venda_id"),
    col("nome").alias("nome (nome do cliente)"),
    col("valor"),
    col("data_venda")
)

# Exibe o DataFrame resultante
df_novo.show()
```

## Dependências

- **PySpark**: A biblioteca do Apache Spark para Python.

## Como Rodar

1. Instale o PySpark, se necessário:
   ```bash
   pip install pyspark
   ```

2. Certifique-se de ter os DataFrames `df_vendas` e `df_clientes` carregados antes de executar o script.

3. Execute o script em seu ambiente de desenvolvimento.

---

Esse README proporciona uma visão clara do que o código faz e como utilizá-lo.