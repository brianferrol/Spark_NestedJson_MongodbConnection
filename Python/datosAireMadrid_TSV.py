import pandas as pd
import requests
import csv
import time
import numpy as np
import io



# API request: 
url = "http://www.mambiente.madrid.es/opendata/horario.csv"



while True:
	# Hacemos la llamada a la API y elegimos el separador del csv
	s=requests.get(url).content
	c=pd.read_csv(io.StringIO(s.decode('utf-8')), sep=';')


	# Creamos las columnas concatenando valor con indicador de validez:
	c['H01'] =  c.V01+c.H01.map(str)
	c['H02'] =  c.V02+c.H02.map(str)
	c['H03'] =  c.V03+c.H03.map(str)
	c['H04'] =  c.V04+c.H04.map(str)
	c['H05'] =  c.V05+c.H05.map(str)
	c['H06'] =  c.V06+c.H06.map(str)
	c['H07'] =  c.V07+c.H07.map(str)
	c['H08'] =  c.V08+c.H08.map(str)
	c['H09'] =  c.V09+c.H09.map(str)
	c['H10'] = c.V10+c.H10.map(str)
	c['H11'] = c.V11+c.H11.map(str)
	c['H12'] = c.V12+c.H12.map(str)
	c['H13'] = c.V13+c.H13.map(str)
	c['H14'] = c.V14+c.H14.map(str)
	c['H15'] = c.V15+c.H15.map(str)
	c['H16'] = c.V16+c.H16.map(str)
	c['H17'] = c.V17+c.H17.map(str)
	c['H18'] = c.V18+c.H18.map(str)
	c['H19'] = c.V19+c.H19.map(str)
	c['H20'] = c.V20+c.H20.map(str)
	c['H21'] = c.V21+c.H21.map(str)
	c['H22'] = c.V22+c.H22.map(str)
	c['H23'] = c.V23+c.H23.map(str)
	c['H24'] = c.V24+c.H24.map(str)



	
	# Borramos las columnas que no nos sirven:
	drop_columns = {'V01', 'V02', 'V03', 'V04', 'V05', 'V06', 'V07', 'V08', 'V09', 'V10', 'V11', 'V12', 'V13', 'V14', 'V15', 'V16', 'V17', 'V18', 'V19', 'V20',                'V21', 'V22', 'V23', 'V24'}
	c.drop(columns=drop_columns, axis=1)



	
	# Guardamos el fichero:
	eventTime = time.strftime("%Y%m%d%H%M")
	file_name = "airQuality_"+eventTime+'.csv'
	c.to_csv(r'./data/'+file_name, sep='\t',encoding='utf-8', index=False)

	# Se ejecuta el while cada hora
	time.sleep(3600)


