import os
import yfinance as yf
from datetime import datetime, timedelta
from prefect import flow, task
from prefect.server.schemas.schedules import CronSchedule

BASE_DIR = "data"
os.makedirs(BASE_DIR, exist_ok=True)

ALL_TICKERS = [
     "ABEV3.SA", "ALPA4.SA", "AMER3.SA", "ARZZ3.SA", "ASAI3.SA", "AZUL4.SA",
"B3SA3.SA", "BBAS3.SA", "BBDC3.SA", "BBDC4.SA", "BBSE3.SA", "BEEF3.SA",
"BPAC11.SA", "BPAN4.SA", "BRAP4.SA", "BRFS3.SA", "BRKM5.SA", "BRML3.SA",
"CASH3.SA", "CCRO3.SA", "CIEL3.SA", "CMIG4.SA", "CMIN3.SA", "COGN3.SA",
"CPFE3.SA", "CPLE6.SA", "CRFB3.SA", "CSAN3.SA", "CSNA3.SA", "CVCB3.SA",
"CYRE3.SA", "DXCO3.SA", "ECOR3.SA", "EGIE3.SA", "ELET3.SA", "ELET6.SA",
"EMBR3.SA", "ENBR3.SA", "ENGI11.SA", "ENEV3.SA", "EQTL3.SA", "EZTC3.SA",
"FLRY3.SA", "GGBR4.SA", "GOAU4.SA", "GOLL4.SA", "HAPV3.SA", "HGTX3.SA",
"HYPE3.SA", "IGTI11.SA", "IRBR3.SA", "ITSA4.SA", "ITUB4.SA", "JBSS3.SA",
"JHSF3.SA", "KLBN11.SA", "LAME4.SA", "LCAM3.SA", "LIGT3.SA", "LINX3.SA",
"LREN3.SA", "MGLU3.SA", "MOVI3.SA", "MRFG3.SA", "MRVE3.SA", "MULT3.SA",
"MYPK3.SA", "NTCO3.SA", "PCAR3.SA", "PETR3.SA", "PETR4.SA", "POSI3.SA",
"PRIO3.SA", "QUAL3.SA", "RADL3.SA", "RAIL3.SA", "RENT3.SA", "RRRP3.SA",
"SANB11.SA", "SBSP3.SA", "SULA11.SA", "SUZB3.SA", "TAEE11.SA", "TIMS3.SA",
"TOTS3.SA", "UGPA3.SA", "USIM5.SA", "VALE3.SA", "VBBR3.SA", "VIVT3.SA",
"VVAR3.SA", "WEGE3.SA", "YDUQ3.SA"
]
INITIAL_TICKERS_COUNT = 12

INITIAL_TICKERS_COUNT = 12  


@task(log_prints=True)
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


@task(log_prints=True)
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


@flow(log_prints=True)
def daily_stock_workflow():
    """
    Workflow diário para baixar e processar dados de ações.
    """
    today = datetime.today()
    start_date = today - timedelta(days=7)
    end_date = today

    print(f"[INFO] Iniciando workflow para o período de {start_date.date()} a {end_date.date()}.")


    tickers = ALL_TICKERS[:INITIAL_TICKERS_COUNT]


    data = download_data(tickers, start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d"))
    if data is None:
        print("[ERRO] Workflow interrompido devido à falha no download.")
        return

    save_partitioned_data(data, start_date)

    print("[INFO] Workflow concluído com sucesso.")




if __name__ == "__main__":
    daily_stock_workflow.serve(name="daily-stock-overflow", cron="0 19 * * *")
