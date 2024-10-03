import pandas as pd

# Lê o arquivo CSV com cabeçalho e delimitador vírgula
df = pd.read_csv("./files/movies.csv")

# Transforma os nomes das colunas para letras minúsculas
df.columns = df.columns.str.lower()

# Converte todas as colunas para o tipo string
df = df.astype(str)

# Salva o DataFrame em um arquivo Parquet
df.to_parquet("./files/movies.parquet")
