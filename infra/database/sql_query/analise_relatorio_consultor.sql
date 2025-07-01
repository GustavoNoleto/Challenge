/*Analise de relatorio tecnico*/
SELECT
    VO1_FILIAL FILIAL,
    VO1_FUNABE CODPRO,
    VO1_NUMOSV OS,
    CONVERT(DATE, ZO4_DATEXE) DATA,
    CASE ZO4_RESPOS
             WHEN '0' THEN 'Mal Preparado'
             WHEN '1' THEN 'Bem Preparado'
             ELSE 'Vazio' 
       END AVALIACAO_RELATORIO
FROM VO1010
    INNER JOIN VAI010 ON VO1_FUNABE = VAI_CODTEC AND VAI010.D_E_L_E_T_ = ''
    INNER JOIN (
        SELECT 
          ZO4_FILIAL,
          ZO4_NUMOSV,
          ZO4_DATEXE,
          IIF(SUM(cast(ZO4_RESPOS as INT)) = COUNT(ZO4_RESPOS),1,0) ZO4_RESPOS
        FROM ZO4010 ZO4
        WHERE ZO4_RESPOS <> ''
          AND ZO4.D_E_L_E_T_ = ''
               AND ZO4_DATEXE = (SELECT MAX(ZO4_DATEXE) 
                                  FROM ZO4010 X 
                                                WHERE X.ZO4_FILIAL = ZO4.ZO4_FILIAL
                                                  AND X.ZO4_NUMOSV = ZO4.ZO4_NUMOSV
                                                  AND X.D_E_L_E_T_ = '')
        GROUP BY ZO4_FILIAL,  ZO4_NUMOSV,  ZO4_DATEXE,  ZO4_USRRES
    ) ZO4 ON VO1_NUMOSV = ZO4_NUMOSV AND VO1_FILIAL = ZO4_FILIAL
WHERE VO1010.D_E_L_E_T_=''
    AND ZO4_DATEXE >= '20231101'
GROUP BY VO1_FILIAL, VO1_FUNABE, VAI_NOMTEC, VO1_NUMOSV,ZO4_RESPOS, ZO4_DATEXE