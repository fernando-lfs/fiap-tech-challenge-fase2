# validar_parquet.py - VERSÃO PARA TESTE LOCAL
import pandas as pd

# Use o nome do arquivo que foi baixado.
LOCAL_FILE_PATH = "raw/3afb163ded5f48348e31ca5491dd219e.snappy.parquet"

print(f"--- INICIANDO VALIDAÇÃO DO ARQUIVO LOCAL: {LOCAL_FILE_PATH} ---")

try:
    # Lê o arquivo Parquet do seu disco local
    df = pd.read_parquet(LOCAL_FILE_PATH)
    df.info()
    print(df.head())
    print("\n--- VALIDAÇÃO LOCAL CONCLUÍDA COM SUCESSO ---")

except Exception as e:
    print(f"\n--- ERRO DURANTE A VALIDAÇÃO LOCAL: {e} ---")
