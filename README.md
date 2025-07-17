# Projeto Bovespa Dados – Requisito 1

Este projeto executa o pipeline ETL inicial para ingestão e processamento de dados da B3 (IBOV), conforme as regras globais e requisitos definidos. O foco é simplicidade, clareza e aderência à Clean Architecture adaptada para projetos pequenos.

## Estrutura de Pastas

```
C:\bovespa_dados
├── README.md
├── pyproject.toml
├── bovespa_etl.ipynb
└── dados
    └── (arquivos CSV do usuário)
```

- `bovespa_etl.ipynb`: Notebook com o pipeline ETL.
- `dados/`: Pasta para armazenar arquivos CSV de pregões, nomeados conforme o padrão.

## Como Executar
1. Instale as dependências com Poetry:
   ```powershell
   poetry install
   ```
2. Abra o notebook `bovespa_etl.ipynb` em seu ambiente Jupyter.
3. Siga as instruções no notebook para carregar e processar o arquivo CSV desejado.

## Dependências
- Python 3.11.9
- poetry
- pandas
- pyarrow
- jupyter

## Observações
- O arquivo CSV deve ser fornecido manualmente pelo usuário e salvo na pasta `dados/`.
- O notebook valida o nome e a estrutura do arquivo antes do processamento.
- O resultado final é salvo em formato Parquet na pasta `dados/`.

Consulte o relatório ao final do notebook para detalhes técnicos, validação e orientações de uso.
