import os
import yfinance as yf
from datetime import datetime, timedelta
from prefect import flow, task
from prefect.artifacts import create_table_artifact
from prefect.blocks.system import JSON

# Configurações
BASE_DIR = "data"
os.makedirs(BASE_DIR, exist_ok=True)

# Carregar lista de tickers de um JSON Block
tickers_block = JSON.load("tickers-list")
ALL_TICKERS = tickers_block.value["tickers"]

@task(log_prints=True, retries=3, retry_delay_seconds=60)
def download_data(tickers, start_date, end_date):
    """
    Baixa os dados de mercado dos tickers especificados.
    """
    try:
        data = yf.download(tickers, start=start_date, end=end_date, group_by="ticker")
        print(f"[INFO] Dados baixados com sucesso para {len(tickers)} tickers.")
        return data
    except Exception as e:
        print(f"[ERRO] Falha ao baixar os dados: {e}")
        return None

@task(log_prints=True, retries=3, retry_delay_seconds=60)
def save_partitioned_data(data, start_date):
    """
    Salva os dados baixados particionados por ticker e data.
    """
    try:
        for i in range(7):
            day = start_date + timedelta(days=i)
            day_str = day.strftime("%Y-%m-%d")
            daily_dir = os.path.join(BASE_DIR, day_str)
            os.makedirs(daily_dir, exist_ok=True)

            for ticker in data.columns.levels[0]:
                ticker_data = data[ticker].loc[day_str:day_str]
                if not ticker_data.empty:
                    file_path = os.path.join(daily_dir, f"{ticker}.csv")
                    ticker_data.to_csv(file_path)
        print(f"[INFO] Dados particionados salvos com sucesso.")
    except Exception as e:
        print(f"[ERRO] Falha ao salvar os dados particionados: {e}")

@task(log_prints=True)
def analyze_stock_performance(data):
    """
    Analisa o desempenho das ações e registra as top 3 que mais subiram e as que mais caíram.
    """
    try:
        latest_date = data.index[-1].date()
        performance = {}

        for ticker in data.columns.levels[0]:
            close_prices = data[ticker]['Close']
            if len(close_prices) > 1:
                change = (close_prices[-1] - close_prices[0]) / close_prices[0]
                performance[ticker] = change * 100  # Percentual

        sorted_perf = sorted(performance.items(), key=lambda x: x[1], reverse=True)

        # Registrar top 3 ações
        create_table_artifact(
            key=f"top-performers-{latest_date}",
            table=[{"Ticker": k, "Variação (%)": v} for k, v in sorted_perf[:3]],
            description=f"Top 3 ações com maior alta em {latest_date}"
        )

        # Registrar piores 3 ações
        create_table_artifact(
            key=f"worst-performers-{latest_date}",
            table=[{"Ticker": k, "Variação (%)": v} for k, v in sorted_perf[-3:]],
            description=f"Top 3 ações com maior queda em {latest_date}"
        )

        print(f"[INFO] Artifacts criados para {latest_date}.")
    except Exception as e:
        print(f"[ERRO] Falha ao analisar desempenho: {e}")
        raise

@flow(log_prints=True)
def daily_stock_workflow():
    """
    Workflow diário para baixar e processar dados de ações.
    """
    today = datetime.today()
    start_date = today - timedelta(days=7)
    end_date = today

    print(f"[INFO] Iniciando workflow para o período de {start_date.date()} a {end_date.date()}.")

    # Baixar dados
    data = download_data(ALL_TICKERS, start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d"))
    if data is None:
        print("[ERRO] Workflow interrompido devido à falha no download.")
        return

    # Salvar dados particionados
    save_partitioned_data(data, start_date)

    # Analisar desempenho das ações
    analyze_stock_performance(data)

    print("[INFO] Workflow concluído com sucesso.")