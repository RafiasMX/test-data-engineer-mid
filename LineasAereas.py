import requests
import pandas as pd
import numpy as np
import DateTime

# Listando las Api Keys necesarias
# 0 Lineas Aereas, 1 Pasajeros 2016, 2 Pasajeros 2017, 3 Vuelos 2016, 4 Vuelos 2017
Api_Keys = ['fed214f3-332d-522c-97ac-da395a066dba','3689da48-d557-5e5f-8347-006ced354939','2a323bb8-0a6d-5bd5-8366-90041c4f1c8c','2743ebad-f1e2-5eff-8c4d-f8c5191d1775','a6960833-d5a3-56dc-b125-da9e4e1fce69']
Cadena_Ini = 'https://analytics.deacero.com/api/teenus/get-data/'
Cadena_Fin = '?format=json'

# response = requests.get(Cadena_Ini+Api_Keys[0]+Cadena_Fin)
# print(response.json())
Lineas_Aereas = pd.json_normalize(requests.get(Cadena_Ini+Api_Keys[0]+Cadena_Fin).json())
Pasajeros_2016 = pd.json_normalize(requests.get(Cadena_Ini+Api_Keys[1]+Cadena_Fin).json())
Pasajeros_2017 = pd.json_normalize(requests.get(Cadena_Ini+Api_Keys[2]+Cadena_Fin).json())
Vuelos_2016 = pd.json_normalize(requests.get(Cadena_Ini+Api_Keys[3]+Cadena_Fin).json())
Vuelos_2017 = pd.json_normalize(requests.get(Cadena_Ini+Api_Keys[4]+Cadena_Fin).json())

# print(Pasajeros_2016)
# print(Pasajeros_2017)

#Pasajeros_2016.to_csv('Pasajeros2016.csv')
#Pasajeros_2017.to_csv('Pasajeros2017.csv')
#Vuelos_2016.to_csv('Vuelos2016.csv')
#Vuelos_2017.to_csv('Vuelos2017.csv')
#Lineas_Aereas.to_csv('LineasAereas.csv')



T1_P2016 = Pasajeros_2016.groupby(["ID_Pasajero"])
print ('\nId duplicados en Pasajeros 2016\n')
print(T1_P2016.size()[T1_P2016.size() > 1])

T2_P2016 = Pasajeros_2016.groupby(["Pasajero"])
print ('\nPasajeros Duplicados 2016\n')
print(T2_P2016.size()[T2_P2016.size() > 1])
#### No se encuentra nada raro en 2016

T3_P2016 = Pasajeros_2016.groupby('ID_Pasajero').Pasajero.nunique()
print ('\nIds con multiples Pasajeros 2016\n')
print(T3_P2016[Pasajeros_2016.groupby('ID_Pasajero').Pasajero.nunique() > 1])

T4_P2016 = Pasajeros_2016.groupby('Pasajero').ID_Pasajero.nunique()
print ('\nPasajeros con multiples Ids 2016\n')
print(T4_P2016[Pasajeros_2016.groupby('Pasajero').ID_Pasajero.nunique() > 1])




T1_P2017 = Pasajeros_2017.groupby(["ID_Pasajero"])
print ('\nId duplicados en Pasajeros 2017\n')
print(T1_P2017.size()[T1_P2017.size() > 1])
#### Se encuentran Id's duplicados

T2_P2017 = Pasajeros_2017.groupby(["Pasajero"])
print ('\nPasajeros con mas de un ID 2017\n')
print(T2_P2017.size()[T2_P2017.size() > 1])
#### Se encuentran pasajeros Duplicados

T3_P2017 = Pasajeros_2017.groupby('ID_Pasajero').Pasajero.nunique()
print ('\nIds con multiples Pasajeros 2017\n')
print(T3_P2017[T3_P2017 > 1])

T4_P2017 = Pasajeros_2017.groupby('Pasajero').ID_Pasajero.nunique()
print ('\nPasajeros con multiples Ids 2017\n')
print(T4_P2017[T4_P2017 > 1])

MultiIdPasajeros_2017 = T3_P2017[T3_P2017 > 1]


#Se quitan los ID's que tenian multiples pasajeros
Pasajeros_2017 = Pasajeros_2017[~Pasajeros_2017.ID_Pasajero.isin(MultiIdPasajeros_2017.index.array)]
#Se quitan los registros duplicados combinando Id_Pasajero con Pasajero
Pasajeros_2017 = Pasajeros_2017.drop_duplicates(subset = ['ID_Pasajero','Pasajero'])

#Revisamos que no haya ID duplicados entre 2016 y 2017
print(pd.merge(Pasajeros_2016,Pasajeros_2017,on='ID_Pasajero'))

#No hay duplicados entre años, entonces se hace el merge

#Unimos las 2 tablas
Pasajeros = Pasajeros_2016.append(Pasajeros_2017)
# Pasajeros.to_csv('Pasajeros.csv')
# print(Pasajeros)

Vuelos = Vuelos_2016.append(Vuelos_2017)
Vuelos['Viaje'] = pd.to_datetime(Vuelos['Viaje'], format='%m/%d/%Y')
#Vuelos.to_csv('Vuelos.csv')

# se confirma que puede haber varios vuelos en la misma ruta en el mismo día
# Un cliente solo puede comprar un boleto por vuelo y solo para el mismo
# Se tienen que identificar los pasajeros que tienen en el mismo día y en la misma ruta mas de 1 registro


Vuelos_Correctos = Vuelos.drop_duplicates(subset = ['Viaje','Ruta','Cve_Cliente'], keep = False)
Vuelos_Duplicados = pd.concat([Vuelos,Vuelos_Correctos]).drop_duplicates(keep = False)
Vuelos_Duplicados['Cve_Cliente'] = np.nan
Vuelos_Final = Vuelos_Correctos.append(Vuelos_Duplicados)


Vuelos_Pasajeros = pd.merge(Vuelos_Final,Pasajeros,left_on='Cve_Cliente',right_on='ID_Pasajero', how='left', validate='many_to_one')
#Vuelos_Pasajeros.to_csv('Vuelos_Pasajeros.csv')

Vuelos_Pasajeros_AL = pd.merge(Vuelos_Pasajeros,Lineas_Aereas,left_on='Cve_LA',right_on='Code', how='left', validate='many_to_one')

Vuelos_Pasajeros_AL.Linea_Aerea = Vuelos_Pasajeros_AL.Linea_Aerea.fillna('Otra')
#Vuelos_Pasajeros_AL.to_csv('Vuelos_Pasajeros_AL.csv')

Reporte_raw = Vuelos_Pasajeros_AL[['Viaje','Clase','Precio','Ruta','Edad','Linea_Aerea']]
#Reporte_raw.to_csv('Reporte.raw.csv')

Reporte = Reporte_raw[['Clase','Precio','Ruta','Linea_Aerea']]
Reporte['Anio'] = Reporte_raw.Viaje.dt.year.astype(str)
Reporte['Semestre'] = np.where(Reporte_raw.Viaje.dt.quarter.gt(2),2,1).astype(str)



#Por último, se requiere el promedio semestral (el primer semestre es de Ene - Jun y el segundo es de Jul - Dic)
# del precio agrupado por Año, Clase, Ruta y las Línea Aérea como columnas.
Reporte.groupby(['Anio','Semestre','Clase','Ruta','Linea_Aerea'])['Precio'].mean().to_csv('Resultado.csv')   #average
print(Reporte.groupby(['Anio','Semestre','Clase','Ruta','Linea_Aerea'])['Precio'].mean())