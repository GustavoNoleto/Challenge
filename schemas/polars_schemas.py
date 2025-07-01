from polars import String, Date, Float64, Int64

class PolarsSchemas():
    
    retrabalho_schema = {
        "FILIAL": String,
        "STATUS": String,
        "OS_CORRECAO": String,
        "DATA": Date,
        "PRODUTIVO": String,
        "NOME": String,
        "OS_ORIGEM": String,
        "RETRABALHO": String,
        "DEPT_INTERNO": String,
        "DESCRICAO": String,
        "TECNICORETRABALHO": String,
        "CODPRO": String
    }
    
    consultores_schema = {
        "FILIAL": String,
        "CODPRO": String,
        "NOM_TEC": String,
        "CPF": String,
        "ADMISSAO": String,
    }
    
    questoes_analise_relatorio_schema = {
        "CODPRO": String,
        "OS": String,
        "DATA": String,
        "CODPER": String,
        "PERGUNTA": String,
        "AVALIACAO_RELATORIO": String
    }
    
    valor_hora_consultores_schema = {
        "CPF": String,
        "CARGO": String,
        "VALOR_HORA": Float64,
        "DATA": Date
    }
    
    epi_consultores_schema = {
        "CPF": String,
        "COD_EPI": String,
        "DESCRICAO_EPI": String,
        "QUANTIDADE_ENTREGUE": Int64,
        "DATA_ENTREGA": Date,
        "QUANTIDADE_DEVOLVIDA": Int64,
        "DATA_DEVOLUCAO": Date,
        "VALOR": Float64
    }
    
    valor_comissoes_consultores_schema = {
        "CPF": String,
        "VALOR": Float64,
        "DATA_REFERENCIA": Date,
        "DATA_PAGAMENTO": Date
    }