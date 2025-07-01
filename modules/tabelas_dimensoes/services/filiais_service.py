from modules.tabelas_dimensoes.repositories.dimensoes_repository import DimensoesDataFrames

class DimensaoFiliais():
    def get_dimensao_filiais():
        filiais_df = DimensoesDataFrames.filiais_df
        
        return filiais_df