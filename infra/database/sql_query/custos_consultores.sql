/*Custos consultores*/
SELECT 
F1_FILIAL FILIAL,
CONVERT(DATE,F1_EMISSAO) DATAEMISSAO,
CONVERT(DATE,F1_DTDIGIT) DATADIGITACAO,
VAI_CODTEC CODPRO,
--A2_NOME NOMEPRODUTIVO,
D1_TOTAL VALORBRUTO,
D1_DOC ROMANEIO,
CASE WHEN D1_COD IN ('VIAGENS','VIAGENS-SERV') THEN 'A SERVICO' ELSE 'EM TREINAMENTO' END TIPODESPESA,
D1_CC CENTRODECUSTO
--CTT_DESC01 DESCCENTRODECUSTO
FROM SF1010 F1
INNER JOIN SD1010 D1 ON F1_FILIAL = D1_FILIAL 
AND F1_DOC = D1_DOC 
AND F1_SERIE = D1_SERIE
AND F1_FORNECE = D1_FORNECE
AND F1_LOJA = D1_LOJA
AND F1_EMISSAO = D1_EMISSAO
AND F1_DTDIGIT = D1_DTDIGIT
AND D1.D_E_L_E_T_ = ''
INNER JOIN SA2010 A2 ON F1_FORNECE = A2_COD
AND F1_LOJA = A2_LOJA
AND A2.D_E_L_E_T_ = ''
LEFT OUTER JOIN CTT010 CTT ON CTT_CUSTO = D1_CC AND CTT.D_E_L_E_T_ = ''
INNER JOIN VAI010 VAI ON VAI_CPF = A2_CGC AND VAI.D_E_L_E_T_ = ''
WHERE 
F1.D_E_L_E_T_ = ''
AND F1_ESPECIE = 'REC'
AND D1_COD IN ('VIAGENS','VIAGENS-SERV','TREINAMENTOCOLABORAD','TREINAMCOLABORAD-SERV')
--AND F1_DTDIGIT >= '20231101'