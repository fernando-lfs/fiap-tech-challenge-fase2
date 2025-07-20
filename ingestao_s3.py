import os
import shutil  # Importa a biblioteca para manipulação de arquivos
import pandas as pd
import awswrangler as wr

# --- Constantes e Configurações ---
S3_BUCKET_NAME = "bucket-bovespa-363362475222"
LOCAL_RAW_DATA_DIR = "dados_brutos"
# Novo diretório para arquivar arquivos já processados
LOCAL_PROCESSED_DATA_DIR = "dados_brutos_processados"
S3_RAW_PREFIX = f"s3://{S3_BUCKET_NAME}/raw/"


def clean_numeric_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Converte colunas numéricas que estão como string com vírgula decimal para float.
    """
    cols_to_convert = ["part", "partAcum", "theoricalQty"]

    for col in cols_to_convert:
        if col in df.columns:
            df[col] = (
                df[col]
                .astype(str)
                .str.replace(".", "", regex=False)
                .str.replace(",", ".", regex=False)
            )
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


def ingest_raw_data_to_s3():
    """
    Lê arquivos CSV do diretório local, limpa, ingere no S3
    em formato Parquet, e move o arquivo local para um diretório de processados.
    """
    print("--- INICIANDO INGESTÃO DE DADOS BRUTOS PARA O S3 ---")

    # Garante que o diretório de arquivos processados exista
    os.makedirs(LOCAL_PROCESSED_DATA_DIR, exist_ok=True)

    csv_files = [f for f in os.listdir(LOCAL_RAW_DATA_DIR) if f.endswith(".csv")]

    if not csv_files:
        print("Nenhum arquivo novo para processar.")
        return

    for file_name in csv_files:
        file_path = os.path.join(LOCAL_RAW_DATA_DIR, file_name)
        processed_file_path = os.path.join(LOCAL_PROCESSED_DATA_DIR, file_name)

        print(f"Processando arquivo: {file_path}")

        try:
            df = pd.read_csv(file_path, sep=";")
            df = clean_numeric_columns(df)

            if "dt_pregao" in df.columns:
                df = df.rename(columns={"dt_pregao": "dt"})
            else:
                print(
                    f"Coluna 'dt_pregao' não encontrada no arquivo {file_name}. Pulando."
                )
                continue

            wr.s3.to_parquet(
                df=df,
                path=S3_RAW_PREFIX,
                dataset=True,
                partition_cols=["dt"],
                mode="overwrite_partitions",
                dtype={"dt": "date"},
            )

            print(f"Sucesso! Dados ingeridos em: {S3_RAW_PREFIX}dt={df['dt'].iloc[0]}/")

            # Move o arquivo processado para o diretório de arquivados
            shutil.move(file_path, processed_file_path)
            print(f"Arquivo local movido para: {processed_file_path}")

        except Exception as e:
            print(f"Erro ao processar o arquivo {file_name}: {e}")

    print("--- INGESTÃO FINALIZADA ---")


if __name__ == "__main__":
    ingest_raw_data_to_s3()
