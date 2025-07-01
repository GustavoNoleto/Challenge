import asyncio
from modules.assertividade_score.repositories.assertividade_repository import AssertividadeDataFrame
from modules.tabelas_dimensoes.repositories.dimensoes_repository import DimensoesDataFrames
from modules.lucratividade_score.repositories.lucratividade_repository import LucratividadeDataFrame
from modules.produtividade_eficiencia_score.repositories.produtividade_eficiencia_repository import ProdutividadeEficienciaDataframe


class TaskScheduler():
    async def scheduler():
        asyncio.create_task(AssertividadeDataFrame.run_scheduled_jobs())
        asyncio.create_task(DimensoesDataFrames.run_scheduled_jobs())
        asyncio.create_task(LucratividadeDataFrame.run_scheduled_jobs())
        asyncio.create_task(ProdutividadeEficienciaDataframe.run_scheduled_jobs())