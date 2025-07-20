# validar_parquet.py
import awswrangler as wr
import pandas as pd

# --- Configuração ---
# Cole aqui o URI do S3 do arquivo Parquet que você copiou do console.
S3_FILE_PATH = "s3://bucket-bovespa-363362475222/raw/dt=2025-07-17/3afb163ded5f48348e31ca5491dd219e.snappy.parquet"

print(f"--- INICIANDO VALIDAÇÃO DO ARQUIVO: {S3_FILE_PATH} ---")

try:
    # 1. Lê um único arquivo Parquet diretamente do S3 para um DataFrame do Pandas.
    print("Lendo arquivo do S3...")
    df = wr.s3.read_parquet(path=[S3_FILE_PATH])

    # 2. Validação do Schema e Tipos de Dados
    print("\n[VALIDAÇÃO 1: Schema e Tipos de Dados (df.info())]")
    # O método .info() é perfeito para uma visão geral: nomes das colunas, contagem de nulos e tipo de dados.
    df.info()

    # 3. Validação do Conteúdo
    print("\n[VALIDAÇÃO 2: Amostra do Conteúdo (df.head())]")
    # O método .head() mostra as 5 primeiras linhas para uma verificação visual rápida.
    print(df.head())

    print("\n--- VALIDAÇÃO CONCLUÍDA COM SUCESSO ---")

except Exception as e:
    print("\n--- ERRO DURANTE A VALIDAÇÃO ---")
    print(f"Ocorreu um erro ao tentar ler o arquivo: {e}")
