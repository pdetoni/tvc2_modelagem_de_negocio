from prefect.deployments import Deployment
from prefect.filesystems import GitHub
from prefect.server.schemas.schedules import CronSchedule
from script import daily_stock_workflow

# Carregar o GitHub Block
github_block = GitHub.load("tvc2")  # Nome do seu GitHub Block

# Criar o deployment
deployment = Deployment.build_from_flow(
    flow=daily_stock_workflow,
    name="daily-stock-deployment",
    storage=github_block,
    work_pool_name="tvc2",  # Nome do seu work pool
    schedule=CronSchedule(cron="0 19 * * *", timezone="America/Sao_Paulo"),
    job_variables={"pip_packages": ["yfinance", "pandas"]}  # Dependências necessárias
)

if __name__ == "__main__":
    deployment.apply()