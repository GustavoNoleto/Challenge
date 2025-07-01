SELECT 
       TDI.CODTEC CODPRO,
       TDI.DATA,
       TDI.HRDISP,
       TTD.TEMTRAB,
       TTF.TEMCOB
FROM
  (
         ----VIEW DE HORA DISPONIVEL POR TECNICO-----
             SELECT DISTINCT 
               VAI_CODTEC CODTEC,
               VOE_DATESC DATA,
               CASE 
                     WHEN VOH_INIREF = 0 
                       THEN VOH_FINPER-VOH_INIPER
                    ELSE (VOH_INIREF-VOH_INIPER+VOH_FINPER-VOH_FINREF) 
               END - ISNULL(TEMAUS,0)  HRDISP
             FROM VAI010 VAI
               INNER JOIN VOE010 VOE ON VAI_CODTEC = VOE_CODPRO AND VOE.D_E_L_E_T_ = ''
               INNER JOIN VOH010 VOH ON VOE_CODPER = VOH_CODPER AND VOH.D_E_L_E_T_ = ''
               LEFT OUTER JOIN (
                    SELECT 
                           VO4_CODPRO CODTEC, 
                           VO4_DATINI DATA,
                           SUM(VO4_TEMAUS) TEMAUS
                           FROM VO4010 
                           WHERE D_E_L_E_T_ = ''
                             AND VO4_TIPAUS IN ('0','2','3')
                           GROUP BY VO4_CODPRO,VO4_DATINI
               ) AS AUS ON VAI_CODTEC = CODTEC AND VOE_DATESC = DATA
             WHERE VAI.D_E_L_E_T_ = ''
               --AND VAI_CODTEC IN ('000031','000352','000574')
               AND CONVERT(DATE,VOE_DATESC) BETWEEN CAST( DATEADD(MONTH,-13,GETDATE()) AS DATE ) AND CAST(GETDATE() AS DATE)
             ---FIM DAS HORAS DISPONIVEIS POR TECNICO-----
       ) AS TDI
  LEFT OUTER JOIN
  (
             ---VIEW DAS HORAS TRABALHADAS DO TECNICO (POR DIA TRABALHADO) ----
             SELECT
                VO4_CODPRO CODTEC, 
                VO4_DATINI DATA,
                SUM(VO4_TEMTRA) TEMTRAB
             FROM VO4010 VO4
             WHERE D_E_L_E_T_ = ''
             GROUP BY VO4_CODPRO,VO4_DATINI
             ---FIM DAS HORAS TRABALHADAS DO TECNICO (POR DIA TRABALHADO) ----
  ) AS TTD ON TDI.CODTEC = TTD.CODTEC AND TDI.DATA = TTD.DATA
  LEFT OUTER JOIN
  (
             ---VIEW DAS HORAS TRABALHADAS DO TECNICO (POR FATURADO) ----
             SELECT
                VO4_CODPRO CODTEC, 
                VO4_DATINI DATA,
                SUM(VO4_TEMCOB) TEMCOB,
                SUM(VO4_TEMTRA) TEMTRAB
             FROM VO4010 VO4
             WHERE D_E_L_E_T_ = ''
             GROUP BY VO4_CODPRO,VO4_DATINI
             ---FIM DAS HORAS TRABALHADAS DO TECNICO (POR FATURADO) ----
  ) AS TTF ON TDI.CODTEC = TTF.CODTEC AND TDI.DATA = TTF.DATA
--WHERE TDI.CODTEC = '000047'
--  AND TDI.DATA = '20240223'
ORDER BY 2,1