SELECT
	'Servicos Faturados'  As TipoRegistro,
	VSC_FILIAL	As CodigoFilial, 
	M0_FILIAL	As DescricaoFilial, 
	M0_ESTENT	As Regiao, 
	M0_NOMECOM	As Linha, 
	VSC_NUMOSV	As NroOS,
	VO1_CHASSI	As Chassi,
	VO1_KILOME	As Horimentro,
	VO1_CODMAR	As CodigoMarca,
	IsNull(VV2_MODVEI,'')	As Modelo,
	IsNull(VV2_DESMOD,'')	As DescricaoModelo,
	VO1_FUNABE As ConsultorAbertura,
	VA3.VAI_NOMTEC As NomeConsultorAbertura,
	VO1_FORPAG As CondPagamento,
	IsNull(E4_DESCRI,'')  As DescriCondPagamento,
	IsNull(VSC.VSC_CODPRO,'') As TecnicoOS,
	IsNull(VA2.VAI_NOMTEC,'') As NomeTecnicoOS,
	IsNull(VA2.VAI_FILPRO,'') As FilialTecnicoOS,
	VSC_NUMNFI	As NroNF, 
	VSC_SERNFI	As SerieNF, 
	IIF(VO1_DATABE <> '', format(cast(VO1_DATABE as date), 'dd/MM/yyyy', 'pt-br'), '') 	As DataAbertura, 
	IIF(VO4_DATDIS <> '', format(cast(VO4_DATDIS as date), 'dd/MM/yyyy', 'pt-br'), '') 	As DataLiberacao, 
	IIF(VO4_DATFEC <> '', format(cast(VO4_DATFEC as date), 'dd/MM/yyyy', 'pt-br'), '') 	As DataFechamento, 
	IIF(VO4_DATCAN <> '', format(cast(VO4_DATCAN as date), 'dd/MM/yyyy', 'pt-br'), '') 	As DataCancelamento,
	SUBSTRING(VO4_DATFEC,5,2) As MesFechamento,
	SUBSTRING(VO4_DATFEC,1,4) As AnoFechamento,
	'Faturado'	As Situacao,
	VSC_TIPTEM	As TipoDeTempo,
	VOI_DESTTE  As DescricaoTTempo, 
	CASE
		WHEN VOI_SITTPO = '1' THEN 'Publico'
		WHEN VOI_SITTPO = '2' THEN 'Garantia'
		WHEN VOI_SITTPO = '3' THEN 'Interno'
		WHEN VOI_SITTPO = '4' THEN 'Revisao'
	END As SituacaoTTpo,
	VSC_GRUSER	As GrupoServico,
	CASE
		WHEN VOK_INCMOB = '0' THEN 'M.Obra Gratuita'
		WHEN VOK_INCMOB = '1' THEN 'M.Obra'
		WHEN VOK_INCMOB = '2' THEN 'Srv.Terceiro'
		WHEN VOK_INCMOB = '3' THEN 'Vlr.Livre c/Base na Tabela'
		WHEN VOK_INCMOB = '4' THEN 'Retorno Srv.'
		WHEN VOK_INCMOB = '5' THEN 'Km Socorro'
		WHEN VOK_INCMOB = '6' THEN 'Franquia'
		WHEN VOK_INCMOB = '7' THEN 'Vlr.Informado'
	END As TipoServico,
	CASE
		WHEN VOK_INCTEM = '1' AND VOK_INCMOB IN ('0','1','3','4') THEN 'Fabrica'                                  
		WHEN VOK_INCTEM = '2' AND VOK_INCMOB IN ('0','1','3','4') THEN 'Concessionaria' 
		WHEN VOK_INCTEM = '3' AND VOK_INCMOB IN ('0','1','3','4') THEN 'Trabalhado' 
		WHEN VOK_INCTEM = '4' AND VOK_INCMOB IN ('0','1','3','4') THEN 'Informado'
		ELSE 'Outros'
	END As TipoCobranca,
	CASE WHEN VO1_TPATEN = '0' THEN 'Interno' ELSE 'Externo' END As TipoAtendimento,
	SA1P.A1_CGC		As CNPJ_CPF_Proprietario,
	SA1P.A1_NOME	As NomeProprietario,
	SA1P.A1_MUN		As CidadeProprietario,
	SA1.A1_CGC		As CNPJ_CPF_Cliente,
	SA1.A1_NREDUZ	As NomeCliente,
	SA1.A1_MUN		As CidadeCliente,
	CASE WHEN VSC_CODPRO <> '' THEN VSC_CODPRO ELSE VO1_FUNABE END As CODPRO,
	CASE WHEN VSC_CODPRO <> '' THEN IsNull(VA2.VAI_NOMTEC,'') ELSE VA3.VAI_NOMTEC END As NomeTecnicoApont,
	CASE WHEN VSC_CODPRO <> '' THEN IsNull(VA2.VAI_FUNCAO,'') + '-' + IsNull(SRJ.RJ_DESC,'') ELSE IsNull(VA3.VAI_FUNCAO,'') + '-' + IsNull(SR2.RJ_DESC,'') END As Funcao,
	VO6_CODSER	As CodigoServico,
	VO6_DESSER	As DescricaoServico,
	CASE WHEN VO6_XCOMI = 'S' THEN 'SIM' ELSE 'NAO' END As [Comssionado?],
	IIF(VO4_DATINI <> '', format(cast(VO4_DATINI as date), 'dd/MM/yyyy', 'pt-br'), '') 	As DtInicioApontamento, 
	IIF(VO4_HORINI <> 0 , LEFT(RTRIM(STR(VO4_HORINI,4,0)),2) + ':' + RIGHT(RTRIM(STR(VO4_HORINI,4,0)),2), '') As HoraInicioApontamento,
	IIF(VO4_DATFIN <> '', format(cast(VO4_DATFIN as date), 'dd/MM/yyyy', 'pt-br'), '') 	As DtFinalApontamento, 
	IIF(VO4_HORINI <> 0 , LEFT(RTRIM(STR(VO4_HORFIN,4,0)),2) + ':' + RIGHT(RTRIM(STR(VO4_HORFIN,4,0)),2), '') As HoraFinalApontamento,
	VSC_TEMPAD	As TempoPadrao,
	VSC_TEMTRA	As TempoTrabalhado,
	VSC_TEMVEN	As TempoVendido,
	VSC_TEMCOB	As TempoCobrado,
	VSC_KILROD	As KmRodado,
	--SUBSTRING(VSC_DATVEN,7,2)+'/'+SUBSTRING(VSC_DATVEN,5,2)+'/'+SUBSTRING(VSC_DATVEN,1,4) DataVenda,
	CONVERT(DATE,LEFT(VSC_DATVEN,6)+'01') DataVenda,
	CASE 
		WHEN VOI_SITTPO = '3' AND VO4_VALVEN > 0 AND VOK_INCMOB = '2' THEN VO4_VALVEN
		WHEN VOI_SITTPO = '3' AND VO4_VALINT > 0 AND VOK_INCMOB != '2' THEN VO4_VALINT
		WHEN VOI_SITTPO = '3' AND VO4_VALINT = 0 AND VO4_VALVEN > 0 THEN VO4_VALVEN 
		ELSE VSC_VALBRU END As ValorBruto,
	VSC_VALDES	As ValorDesconto,
	CASE 
		WHEN VOI_SITTPO = '3' AND VO4_VALVEN > 0 AND VOK_INCMOB = '2' THEN VO4_VALVEN
		WHEN VOI_SITTPO = '3' AND VO4_VALINT > 0 AND VOK_INCMOB != '2' THEN VO4_VALINT 
		WHEN VOI_SITTPO = '3' AND VO4_VALINT = 0 AND VO4_VALVEN > 0 THEN VO4_VALVEN 
		ELSE VSC_VALBRU-VSC_VALDES  END As ValorServico,	
	CASE WHEN VOK_INCMOB = '2' THEN 0 ELSE VSC_VALISS	END As ValorISS,
	CASE WHEN VOK_INCMOB = '2' THEN 0 ELSE VSC_VALPIS	END As ValorPIS,
	CASE WHEN VOK_INCMOB = '2' THEN 0 ELSE VSC_VALCOF	END As ValorCOFINS,
	CASE WHEN VOK_INCMOB = '2' THEN 0 ELSE VSC_TOTIMP	END As TotalImpostos,
	CASE WHEN VOK_INCMOB = '2' THEN VO4_VALVEN ELSE VSC_CUSTOT	END As CustoTotal,
	CASE WHEN VOK_INCMOB = '2' THEN VO4_VALVEN ELSE VSC_CUSSER	END As CustoServico,
	CASE 
		WHEN VOI_SITTPO = '3' AND VO4_VALVEN > 0 AND VOK_INCMOB = '2' THEN VO4_VALVEN
		WHEN VOI_SITTPO = '3' AND VOK_INCMOB != '2' AND VO4_VALINT > 0 THEN VO4_VALINT 
		ELSE VSC_LUCBRU END As LucroBruto,
	CASE WHEN VOI_SITTPO = '3' AND VO4_VALVEN > 0 AND VOK_INCMOB = '2' THEN VO4_VALVEN
		WHEN VOI_SITTPO = '3' AND VOK_INCMOB != '2' AND VO4_VALINT > 0 THEN VO4_VALINT  ELSE VSC_LUCLIQ END As LucroLiquido,
	CASE WHEN VOI_SITTPO = '3' AND VO4_VALVEN > 0 AND VOK_INCMOB = '2' THEN VO4_VALVEN
		WHEN VOI_SITTPO = '3' AND VOK_INCMOB != '2' AND VO4_VALINT > 0 THEN VO4_VALINT  ELSE VSC_RESFIN END As ResultadoFinal,
	CASE WHEN VO6_XDZSUP = 'S' THEN 'Sim' ELSE 'Não' END	As PontuaSuperAcao,
	CASE WHEN VO6_XDZFID = 'S' THEN 'Sim' ELSE 'Não' END	As PontuaJDFidelidade,
	VO4_DEPINT	As DptoInterno,
	IsNull(SX5DINT.X5_DESCRI, '') NomeDptoInterno,
	VO1_XTPOSI As TipoOsInterna,
	CASE 
		WHEN VO1_OROSJD in ('0','') THEN 'Oficina'
		ELSE 'CSC' 
	 END AS Origem_OS


FROM VSC010 VSC (NOLOCK) 
INNER JOIN VO1010 VO1 (NOLOCK) ON VO1_FILIAL = VSC_FILIAL AND VO1_NUMOSV = VSC_NUMOSV AND VO1.D_E_L_E_T_=''
INNER JOIN VO4010 VO4 (NOLOCK) ON VO4_FILIAL = VSC_FILIAL AND VO4_NUMOSV = VSC_NUMOSV AND VO4_TIPTEM = VSC_TIPTEM AND VSC_NUMIDE = VO4_VSCIDE AND VO4.D_E_L_E_T_=''
INNER JOIN VO6010 VO6 (NOLOCK) ON VO6_FILIAL = '0101' AND VO6_CODSER = VO4_CODSER AND VO6_GRUSER = VO4_GRUSER AND VO6.D_E_L_E_T_=''
INNER JOIN VOK010 VOK (NOLOCK) ON SUBSTRING(VOK_FILIAL,1,2) = SUBSTRING(VSC_FILIAL,1,2) AND VSC_TIPSER = VOK_TIPSER AND VOK.D_E_L_E_T_=''
INNER JOIN VOI010 VOI (NOLOCK) ON SUBSTRING(VOI_FILIAL,1,2) = SUBSTRING(VSC_FILIAL,1,2) AND VSC_TIPTEM = VOI_TIPTEM AND VOI.D_E_L_E_T_=''
INNER JOIN SA1010 SA1 (NOLOCK) ON A1_FILIAL  = '' AND A1_COD = VO4_FATPAR AND A1_LOJA = VO4_LOJA AND SA1.D_E_L_E_T_=''
INNER JOIN VV1010 VV1 (NOLOCK) ON VV1_FILIAL = '0101' AND VV1_CHAINT = VO1_CHAINT AND VV1.D_E_L_E_T_=''
LEFT  JOIN VV2010 VV2 (NOLOCK) ON VV2_FILIAL = '0101' AND VV2_CODMAR = VV1_CODMAR AND VV2_MODVEI = VV1_MODVEI AND VV2_SEGMOD = VV1_SEGMOD AND VV2.D_E_L_E_T_=''
LEFT  JOIN VAI010 VAI (NOLOCK) ON SUBSTRING(VAI.VAI_FILIAL,1,4) = SUBSTRING(VO1_FILIAL,1,4) AND VO1_XCODME = VAI.VAI_CODTEC AND VAI.D_E_L_E_T_=''
LEFT  JOIN VAI010 VA2 (NOLOCK) ON SUBSTRING(VA2.VAI_FILIAL,1,4) = SUBSTRING(VO1_FILIAL,1,4) AND VSC_CODPRO = VA2.VAI_CODTEC AND VA2.D_E_L_E_T_=''
LEFT  JOIN VAI010 VA3 (NOLOCK) ON SUBSTRING(VA3.VAI_FILIAL,1,4) = SUBSTRING(VO1_FILIAL,1,4) AND VO1_FUNABE = VA3.VAI_CODTEC AND VA3.D_E_L_E_T_=''
LEFT  JOIN SRJ010 SRJ (NOLOCK) ON SRJ.RJ_FILIAL = '0101' AND SRJ.RJ_FUNCAO = VA2.VAI_FUNCAO AND SRJ.D_E_L_E_T_=''
LEFT  JOIN SRJ010 SR2 (NOLOCK) ON SR2.RJ_FILIAL = '0101' AND SR2.RJ_FUNCAO = VA3.VAI_FUNCAO AND SR2.D_E_L_E_T_=''
LEFT  JOIN SE4010 SE4 (NOLOCK) ON E4_FILIAL = '' AND E4_CODIGO = VO1_FORPAG AND SE4.D_E_L_E_T_=''
INNER JOIN SYS_COMPANY SM0 (NOLOCK) ON VSC_FILIAL = M0_CODFIL AND SM0.D_E_L_E_T_=''
LEFT JOIN SA1010 SA1P (NOLOCK) ON SA1P.A1_FILIAL  = '' AND SA1P.A1_COD = VV1_PROATU AND SA1P.A1_LOJA = VV1_LJPATU AND SA1.D_E_L_E_T_=''
LEFT  JOIN SX5010 SX5DINT (NOLOCK) ON SX5DINT.X5_FILIAL = '' AND SX5DINT.X5_TABELA = 'VD' AND SX5DINT.X5_CHAVE = VO4_DEPINT AND SX5DINT.D_E_L_E_T_=''

WHERE
	VSC.D_E_L_E_T_=''
--AND VO4_DATFEC >= '20231101'
	AND VO6_XCOMI = 'S'