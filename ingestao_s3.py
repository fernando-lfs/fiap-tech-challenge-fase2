# ingestao_s3.py

import os
import pandas as pd
import awswrangler as wr

# --- Constantes e Configurações ---
# Use o nome do seu bucket S3.
S3_BUCKET_NAME = "bucket-bovespa-363362475222"
# Diretório local onde os arquivos CSV do Requisito 1 estão salvos.
LOCAL_RAW_DATA_DIR = "dados_brutos"
# Prefixo no S3 para os dados brutos.
S3_RAW_PREFIX = f"s3://{S3_BUCKET_NAME}/raw/"


def clean_numeric_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Converte colunas numéricas que estão como string com vírgula decimal para float.

    Args:
        df: DataFrame com os dados brutos.

    Returns:
        DataFrame com as colunas numéricas corrigidas.
    """
    # Lista de colunas a serem convertidas. Adicione outras se necessário.
    cols_to_convert = ["part", "partAcum", "theoricalQty"]

    for col in cols_to_convert:
        if col in df.columns:
            # Substitui o ponto de milhar e a vírgula decimal
            df[col] = (
                df[col]
                .astype(str)
                .str.replace(".", "", regex=False)
                .str.replace(",", ".", regex=False)
            )
            # Converte para float, tratando erros
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


def ingest_raw_data_to_s3():
    """
    Lê arquivos CSV do diretório local, limpa, e ingere no S3
    em formato Parquet e particionado por data.
    """
    print("--- INICIANDO INGESTÃO DE DADOS BRUTOS PARA O S3 ---")

    # Verifica se o diretório de dados brutos existe
    if not os.path.isdir(LOCAL_RAW_DATA_DIR):
        print(f"Erro: O diretório '{LOCAL_RAW_DATA_DIR}' não foi encontrado.")
        print("Certifique-se de executar o script de extração (Requisito 1) primeiro.")
        return

    # Lista todos os arquivos CSV no diretório
    csv_files = [f for f in os.listdir(LOCAL_RAW_DATA_DIR) if f.endswith(".csv")]

    if not csv_files:
        print("Nenhum arquivo CSV encontrado para ingestão.")
        return

    for file_name in csv_files:
        file_path = os.path.join(LOCAL_RAW_DATA_DIR, file_name)
        print(f"Processando arquivo: {file_path}")

        try:
            # 1. Lê o arquivo CSV bruto
            # O separador é ponto e vírgula, conforme o arquivo de progresso.
            df = pd.read_csv(file_path, sep=";")

            # 2. Realiza a limpeza mínima dos tipos de dados
            df = clean_numeric_columns(df)

            # 3. Renomeia a coluna de data para 'dt' para um particionamento limpo
            # O nome da partição será 'dt=YYYY-MM-DD'
            if "dt_pregao" in df.columns:
                df = df.rename(columns={"dt_pregao": "dt"})
            else:
                print(f"Aviso: Coluna 'dt_pregao' não encontrada em {file_name}.")
                continue

            # 4. Ingestão no S3 usando AWS Data Wrangler
            wr.s3.to_parquet(
                df=df,
                path=S3_RAW_PREFIX,
                dataset=True,  # Essencial para criar um "dataset" com partições
                partition_cols=["dt"],  # Coluna usada para particionar
                mode="overwrite_partitions",  # Sobrescreve partições existentes se o script for executado novamente
                dtype={"dt": "date"},  # Garante que a partição seja tratada como data
            )

            print(
                f"Sucesso! Dados do arquivo '{file_name}' foram ingeridos em: {S3_RAW_PREFIX}"
            )

        except Exception as e:
            print(f"Erro ao processar o arquivo {file_name}: {e}")

    print("--- INGESTÃO FINALIZADA ---")


if __name__ == "__main__":
    ingest_raw_data_to_s3()
