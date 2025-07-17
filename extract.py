import base64
import json
import os
from datetime import date

import pandas as pd
import requests

# --- Constantes e Configuração ---
BASE_URL = (
    "https://sistemaswebb3-listados.b3.com.br/indexProxy/indexCall/GetPortfolioDay/"
)
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Accept": "application/json",
}
OUTPUT_DIR = "dados_brutos"


def extrair_dados_pregao(data_pregao: date):
    """
    Extrai os dados de um pregão específico da B3, salva em CSV e retorna o caminho do arquivo.
    NOTA: A API da B3 parece retornar sempre os dados do dia atual, ignorando a data enviada.

    Args:
        data_pregao: Um objeto date representando o dia para nomear o arquivo de saída.

    Returns:
        O caminho do arquivo CSV gerado ou None se não houver dados.
    """
    data_formatada = data_pregao.strftime("%Y-%m-%d")
    print(f"Iniciando extração para o dia: {data_formatada}")

    # 1. Monta o payload. A API provavelmente usará a data atual, mas mantemos o parâmetro.
    payload = {
        "language": "pt-br",
        "pageNumber": 1,
        "pageSize": 120,
        "date": data_formatada,
        "index": "IBOV",
    }

    # 2. Codifica o payload em Base64.
    json_payload = json.dumps(payload)
    encoded_payload = base64.b64encode(json_payload.encode("utf-8")).decode("utf-8")

    # 3. Monta a URL final.
    url = f"{BASE_URL}{encoded_payload}"

    try:
        # 4. Faz a requisição GET.
        response = requests.get(url, headers=HEADERS, timeout=30)
        response.raise_for_status()

        # 5. Processa a resposta JSON.
        data = response.json()
        resultados = data.get("results")

        if not resultados:
            print(f"Nenhum dado encontrado para o dia {data_formatada}.")
            return None

        # 6. Converte para DataFrame e salva em CSV.
        df = pd.DataFrame(resultados)
        df["dt_pregao"] = data_formatada

        os.makedirs(OUTPUT_DIR, exist_ok=True)

        output_path = os.path.join(OUTPUT_DIR, f"pregao_{data_formatada}.csv")
        df.to_csv(output_path, index=False, sep=";")

        print(f"Dados salvos com sucesso em: {output_path}")
        return output_path

    except requests.exceptions.RequestException as e:
        print(f"Erro ao fazer a requisição para o dia {data_formatada}: {e}")
        return None
    except (KeyError, json.JSONDecodeError) as e:
        print(f"Erro ao processar o JSON de resposta para o dia {data_formatada}: {e}")
        return None


if __name__ == "__main__":
    # --- Execução Principal Simplificada ---
    # Este script agora extrai os dados do dia atual (ou último pregão disponível).
    # Para cumprir o requisito, execute este script uma vez por dia durante 7 dias úteis.

    print("--- INICIANDO EXTRAÇÃO DE DADOS DA B3 (DIA ATUAL) ---")

    data_hoje = date.today()

    # Verifica se hoje é um dia útil (Segunda a Sexta).
    if data_hoje.weekday() < 5:
        extrair_dados_pregao(data_hoje)
    else:
        print(
            f"Hoje é {data_hoje.strftime('%Y-%m-%d')}, um final de semana. Nenhum dado de pregão para extrair."
        )

    print("--- EXTRAÇÃO FINALIZADA ---")
