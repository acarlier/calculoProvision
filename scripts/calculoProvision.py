# -*- coding: utf-8 -*-

import pyodbc
import MySQLdb
import sys
sys.path.append('/home/joel/proyectos/calculoProvision/scripts')
sys.path.append('/home/joel/proyectos/conexiones/')
import time
import sqlserverconf
import mysqlconf
from dateutil.relativedelta import relativedelta
from datetime import date

#Conexion a base de datos local mysql
conmy = MySQLdb.connect(charset='utf8',user=str(mysqlconf.get_user("local")),passwd=str(mysqlconf.get_password("local")),host=str(mysqlconf.get_server("local")),db=str(mysqlconf.get_database("logistica")))
cursormy = conmy.cursor()

#Conexion a base de datos netezza
connz = pyodbc.connect("DSN=NZTSLCV")
cursornz = connz.cursor()

#Consulto centros y ultimo inventario tomado para ellos
queryCentros="SELECT distinct centro FROM INVENTARIO.calculoFinalTasaProvision"
cursormy.execute(queryCentros)
resultadoCentros = cursormy.fetchall()

#Calculo de mes en curso (dia anterior)
fechaPeriodoFin = date.today()
fechaPeriodoFin = fechaPeriodoFin - (relativedelta(days=fechaPeriodoFin.day))
fechaPeriodoInicio = fechaPeriodoFin - relativedelta(months=1)
fechaProvisionAnterior = fechaPeriodoInicio
fechaPeriodoInicio = fechaPeriodoInicio + relativedelta(days=1)

print("Periodo a consultar: " + str(fechaPeriodoInicio) + " - " + str(fechaPeriodoFin))

#Calculo de Venta y Brecha invisible en base a su fecha
for centro in resultadoCentros:
	#Consulto centros y ultimo inventario tomado para ellos
	queryDataCategoria="SELECT * FROM INVENTARIO.calculoFinalTasaProvision WHERE centro = '" + str(centro[0]) + "'"
	cursormy.execute(queryDataCategoria)
	resultadoDataCategoria = cursormy.fetchall()
	tasaPorCategoria = {}
	for dataCategoria in resultadoDataCategoria:
		tasaPorCategoria[dataCategoria[1]] = dataCategoria
	if dataCategoria[2] >= fechaPeriodoInicio and dataCategoria[2] <= fechaPeriodoFin:
		print("Inventario dentro del mes")
		# Tiene inventario durante el mes, por lo que se provisiona solo periodo posterior a inventario
		queryResultadoMes = "SELECT CENTRO, 		CATEGORIA,		SUM(VENTA_NETA) as VENTA_NETA,		SUM(BRECHA_VISIBLE) as BRECHA_VISIBLE,		SUM(AJUSTE_MANUAL) as AJUSTE_MANUAL,		SUM(BRECHA_INVISIBLE) as BRECHA_INVISIBLE FROM (SELECT CENTRO, 			JER_COD_CAT AS CATEGORIA,			'0' AS VENTA_NETA,	 		SUM(CASE WHEN CONCEPTO_CLASE_MOV IN ('02') AND MOTIVO NOT IN ('0061') THEN VAL_COMPRA_MON_LOC ELSE 0 END) AS BRECHA_VISIBLE,		SUM(CASE WHEN CONCEPTO_CLASE_MOV IN ('14') THEN VAL_COMPRA_MON_LOC ELSE 0 END) AS AJUSTE_MANUAL,			SUM(CASE WHEN CONCEPTO_CLASE_MOV IN ('03') THEN VAL_COMPRA_MON_LOC ELSE 0 END) AS BRECHA_INVISIBLE 	FROM NZ_BU.PP_CALIDAD.VW_BW_MOVIMIENTOS_INVENTARIO_MOTIVO  			LEFT JOIN NZ_HISTORICO.BW.BW_JER_MAT			ON 1*NZ_BU.PP_CALIDAD.VW_BW_MOVIMIENTOS_INVENTARIO_MOTIVO.MATERIAL = NZ_HISTORICO.BW.BW_JER_MAT.JER_COD_MAT	WHERE CENTRO = '" + str(dataCategoria[0]) + "'  			AND CENTRO_COSTO <> '' 		AND SUBSTR(CENTRO_COSTO,5, 4) = CENTRO		AND DIA_NATURAL BETWEEN '" + str(dataCategoria[2]) + "' AND '" + str(fechaPeriodoFin) + "'		GROUP BY CENTRO, JER_COD_CAT 	UNION ALL SELECT CENTRO, 			JER_COD_CAT AS CATEGORIA,			SUM(VTA_NETA) AS VENTA_NETA,		'0' AS BRECHA_VISIBLE,		'0' AS AJUSTE_MANUAL,		'0' AS BRECHA_INVISIBLE	FROM PP_CALIDAD.VW_BW_VENTAS		LEFT JOIN NZ_HISTORICO.BW.BW_JER_MAT		ON 1*PP_CALIDAD.VW_BW_VENTAS.MATERIAL = NZ_HISTORICO.BW.BW_JER_MAT.JER_COD_MAT	WHERE CENTRO = '" + str(dataCategoria[0]) + "'		AND FECHA_VENTA BETWEEN '" + str(dataCategoria[2]) + "' AND '" + str(fechaPeriodoFin) + "'		GROUP BY CENTRO, JER_COD_CAT) vistaTemporal GROUP BY CENTRO,CATEGORIA"
		# print(queryResultadoMes)
		cursornz.execute(queryResultadoMes)
		resultadoResultadoMes = cursornz.fetchall()
		resultadosPorCategoria = {}
		for row in resultadoResultadoMes:
			resultadosPorCategoria[unicode(row[1])] = row
	else:
		print("Sin inventario dentro del mes")
		# No iene inventario durante el mes, se provisiona mes completo
		queryResultadoMes = "SELECT CENTRO, 		CATEGORIA,		SUM(VENTA_NETA) as VENTA_NETA,		SUM(BRECHA_VISIBLE) as BRECHA_VISIBLE,		SUM(AJUSTE_MANUAL) as AJUSTE_MANUAL,		SUM(BRECHA_INVISIBLE) as BRECHA_INVISIBLE FROM (SELECT CENTRO, 			JER_COD_CAT AS CATEGORIA,			'0' AS VENTA_NETA,	 		SUM(CASE WHEN CONCEPTO_CLASE_MOV IN ('02') AND MOTIVO NOT IN ('0061') THEN VAL_COMPRA_MON_LOC ELSE 0 END) AS BRECHA_VISIBLE,		SUM(CASE WHEN CONCEPTO_CLASE_MOV IN ('14') THEN VAL_COMPRA_MON_LOC ELSE 0 END) AS AJUSTE_MANUAL,			SUM(CASE WHEN CONCEPTO_CLASE_MOV IN ('03') THEN VAL_COMPRA_MON_LOC ELSE 0 END) AS BRECHA_INVISIBLE 	FROM NZ_BU.PP_CALIDAD.VW_BW_MOVIMIENTOS_INVENTARIO_MOTIVO  			LEFT JOIN NZ_HISTORICO.BW.BW_JER_MAT			ON 1*NZ_BU.PP_CALIDAD.VW_BW_MOVIMIENTOS_INVENTARIO_MOTIVO.MATERIAL = NZ_HISTORICO.BW.BW_JER_MAT.JER_COD_MAT	WHERE CENTRO = '" + str(dataCategoria[0]) + "'  			AND CENTRO_COSTO <> '' 		AND SUBSTR(CENTRO_COSTO,5, 4) = CENTRO		AND DIA_NATURAL BETWEEN '" + str(fechaPeriodoInicio) + "' AND '" + str(fechaPeriodoFin) + "'		GROUP BY CENTRO, JER_COD_CAT 	UNION ALL SELECT CENTRO, 			JER_COD_CAT AS CATEGORIA,			SUM(VTA_NETA) AS VENTA_NETA,		'0' AS BRECHA_VISIBLE,		'0' AS AJUSTE_MANUAL,		'0' AS BRECHA_INVISIBLE	FROM PP_CALIDAD.VW_BW_VENTAS		LEFT JOIN NZ_HISTORICO.BW.BW_JER_MAT		ON 1*PP_CALIDAD.VW_BW_VENTAS.MATERIAL = NZ_HISTORICO.BW.BW_JER_MAT.JER_COD_MAT	WHERE CENTRO = '" + str(dataCategoria[0]) + "'		AND FECHA_VENTA BETWEEN '" + str(fechaPeriodoInicio) + "' AND '" + str(fechaPeriodoFin) + "'		GROUP BY CENTRO, JER_COD_CAT) vistaTemporal GROUP BY CENTRO,CATEGORIA"
		# print(queryResultadoMes)
		cursornz.execute(queryResultadoMes)
		resultadoResultadoMes = cursornz.fetchall()
		resultadosPorCategoria = {}
		for row in resultadoResultadoMes:
			resultadosPorCategoria[unicode(row[1])] = row
	for key in tasaPorCategoria.keys():
		if tasaPorCategoria[key][2] >= fechaPeriodoInicio and tasaPorCategoria[key][2] <= fechaPeriodoFin:
			try:
				ventaNeta = resultadosPorCategoria[key][2]
			except:
				ventaNeta = 0
			try:
				ajustes = resultadosPorCategoria[key][4]
			except:
				ajustes = 0
			provisionAcumulada = 0
			if tasaPorCategoria[key][15] == None:
				provisionBruta = 0
			else:
				provisionBruta = (-1) * tasaPorCategoria[key][15] * ventaNeta
			print("centro: " + str(centro[0]) + ", provisionAcumulada: " + str(provisionAcumulada) + ", provisionBruta: " + str(provisionBruta) + ", ajustes: " + str(ajustes))
		else:
			try:
				ventaNeta = resultadosPorCategoria[key][2]
			except:
				ventaNeta = 0
			try:
				ajustes = resultadosPorCategoria[key][4]
			except:
				ajustes = 0
			provisionAcumulada = 999999
			if tasaPorCategoria[key][15] == None:
				provisionBruta = 0
			else:
				provisionBruta = (-1) * tasaPorCategoria[key][15] * ventaNeta
			print("centro: " + str(centro[0]) + ", provisionAcumulada: " + str(provisionAcumulada) + ", provisionBruta: " + str(provisionBruta) + ", ajustes: " + str(ajustes))
		# Insertando a tabla de manejo de provision

		queryInsertacalculoProvisionMes = "INSERT INTO INVENTARIO.provision (centro, fecha, categoria, provisionAcumulada, provisionBruta, ajustes) VALUES ('" + str(centro[0]) + "','" + str(fechaPeriodoFin) + "','" + str(key) + "','" + str(provisionAcumulada) + "','" + str(provisionBruta) + "','" + str(ajustes) + "') ON DUPLICATE KEY UPDATE centro = '" + str(centro[0]) + "', fecha = '" + str(fechaPeriodoFin) + "', categoria = '" + str(key) + "', provisionAcumulada = '" + str(provisionAcumulada) + "', provisionBruta = '" + str(provisionBruta) + "', ajustes = '" + str(ajustes) + "'"
		cursormy.execute(queryInsertacalculoProvisionMes)
		conmy.commit()
