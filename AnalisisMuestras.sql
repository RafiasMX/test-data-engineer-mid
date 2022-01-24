----- QUERY 1
--Obtiene todos los sitios que no tienen nombre disponible

SELECT  DS.[clave_sitio]
        ,'Nombre no disponible' as [nombre_sitio]
        ,MU.[clave_monitoreo]
        ,MU.[fecha_realizacion]
        ,MU.[anio]
        ,MU.[co3]
        ,MU.[hco3]
        ,MU.[oh]
        ,MU.[cot]
        ,MU.[cot_sol]
        ,MU.[n_nh3]
        ,MU.[n_no2]  
FROM [TMP].[dbo].[muestras] AS MU
LEFT JOIN [TMP].[dbo].[sitios] AS DS
    ON MU.[clave_sitio] = DS.[clave_sitio]
	WHERE DS.[nombre_sitio] IS NULL


----- QUERY 2
--Obtiene para el 2019 el promedio de todas las mediciones por clave sitio, nombre sitio, clave de monitoreo, fecha y año de realización donde el promedio de n_no2 fuera mayor a 0.05

SELECT  MU.[clave_sitio]
        ,CASE
            WHEN DS.[nombre_sitio] IS NOT NULL THEN DS.[nombre_sitio]
            ELSE 'Nombre no disponible'
        END as [nombre_sitio]
        ,MU.[clave_monitoreo]
        ,MU.[fecha_realizacion]
        ,MU.[anio]
        ,AVG(MU.[co3])
        ,AVG(MU.[hco3])
        ,AVG(MU.[oh])
        ,AVG(MU.[cot])
        ,AVG(MU.[cot_sol])
        ,AVG(MU.[n_nh3])
        ,AVG(MU.[n_no2]) 
FROM [TMP].[dbo].[muestras] AS MU
LEFT JOIN [TMP].[dbo].[sitios] AS DS
    ON MU.[clave_sitio] = DS.[clave_sitio]
WHERE anio = 2019
GROUP BY 
		MU.[clave_sitio]
        ,CASE
            WHEN DS.[nombre_sitio] IS NOT NULL THEN DS.[nombre_sitio]
            ELSE 'Nombre no disponible'
        END
        ,MU.[clave_monitoreo]
        ,MU.[fecha_realizacion]
        ,MU.[anio]
HAVING AVG(MU.[n_no2]) > 0.5

---Realizar una consulta donde podamos visualizar por año, clave y nombre de sitio el promedio de los químicos cuando el valor promedio de [n_no2] es mayor a uno. Cuando no se tiene nombre del sitio debera ponerse el texto 'Nombre no disponible'.

SELECT  MU.[anio]
		,MU.[clave_sitio]
        ,CASE
            WHEN DS.[nombre_sitio] IS NOT NULL THEN DS.[nombre_sitio]
            ELSE 'Nombre no disponible'
        END as [nombre_sitio]
        ,AVG(MU.[co3])
        ,AVG(MU.[hco3])
        ,AVG(MU.[oh])
        ,AVG(MU.[cot])
        ,AVG(MU.[cot_sol])
        ,AVG(MU.[n_nh3])
        ,AVG(MU.[n_no2]) 
FROM [TMP].[dbo].[muestras] AS MU
LEFT JOIN [TMP].[dbo].[sitios] AS DS
    ON MU.[clave_sitio] = DS.[clave_sitio]
GROUP BY 
		MU.[anio]
		,MU.[clave_sitio]
        ,CASE
            WHEN DS.[nombre_sitio] IS NOT NULL THEN DS.[nombre_sitio]
            ELSE 'Nombre no disponible'
        END
HAVING AVG(MU.[n_no2]) > 1

--Realizar una consulta donde se obtenga por año, clave y nombre de sitio la mediana de cualquier medicion quimica. Cuando no se tiene nombre del sitio debera ponerse el texto 'Nombre no disponible'.

--En Hive
SELECT  MU.[anio]
		,MU.[clave_sitio]
        ,CASE
            WHEN DS.[nombre_sitio] IS NOT NULL THEN DS.[nombre_sitio]
            ELSE 'Nombre no disponible'
        END as [nombre_sitio]
        ,percentile(cast(MU.[co3] * 100 as bigint),0.5)/100
        ,percentile(cast(MU.[hco3] * 100 as bigint),0.5)/100
        ,percentile(cast(MU.[oh] * 100 as bigint),0.5)/100
        ,percentile(cast(MU.[cot] * 100 as bigint),0.5)/100
        ,percentile(cast(MU.[cot_sol] * 100 as bigint),0.5)/100
        ,percentile(cast(MU.[n_nh3] * 100 as bigint),0.5)/100
        ,percentile(cast(MU.[n_no2] * 100 as bigint),0.5)/100
FROM [TMP].[dbo].[muestras] AS MU
LEFT JOIN [TMP].[dbo].[sitios] AS DS
    ON MU.[clave_sitio] = DS.[clave_sitio]
GROUP BY 
		MU.[anio]
		,MU.[clave_sitio]
        ,CASE
            WHEN DS.[nombre_sitio] IS NOT NULL THEN DS.[nombre_sitio]
            ELSE 'Nombre no disponible'
        END


--En MySQL
SELECT  distinct
		MU.[anio]
		,MU.[clave_sitio]
        ,CASE
            WHEN DS.[nombre_sitio] IS NOT NULL THEN DS.[nombre_sitio]
            ELSE 'Nombre no disponible'
        END as [nombre_sitio]
        ,PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY MU.[co3]) OVER (PARTITION BY MU.[anio], MU.[clave_sitio], CASE WHEN DS.[nombre_sitio] IS NOT NULL THEN DS.[nombre_sitio] ELSE 'Nombre no disponible' END) AS Median_co3
        ,PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY MU.[hco3]) OVER (PARTITION BY MU.[anio], MU.[clave_sitio], CASE WHEN DS.[nombre_sitio] IS NOT NULL THEN DS.[nombre_sitio] ELSE 'Nombre no disponible' END) AS Median_hco3
        ,PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY MU.[oh]) OVER (PARTITION BY MU.[anio], MU.[clave_sitio], CASE WHEN DS.[nombre_sitio] IS NOT NULL THEN DS.[nombre_sitio] ELSE 'Nombre no disponible' END) AS Median_oh
        ,PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY MU.[cot]) OVER (PARTITION BY MU.[anio], MU.[clave_sitio], CASE WHEN DS.[nombre_sitio] IS NOT NULL THEN DS.[nombre_sitio] ELSE 'Nombre no disponible' END) AS Median_cot
        ,PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY MU.[cot_sol]) OVER (PARTITION BY MU.[anio], MU.[clave_sitio], CASE WHEN DS.[nombre_sitio] IS NOT NULL THEN DS.[nombre_sitio] ELSE 'Nombre no disponible' END) AS Median_cot_sol
        ,PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY MU.[n_nh3]) OVER (PARTITION BY MU.[anio], MU.[clave_sitio], CASE WHEN DS.[nombre_sitio] IS NOT NULL THEN DS.[nombre_sitio] ELSE 'Nombre no disponible' END) AS Median_n_nh3
        ,PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY MU.[n_no2]) OVER (PARTITION BY MU.[anio], MU.[clave_sitio], CASE WHEN DS.[nombre_sitio] IS NOT NULL THEN DS.[nombre_sitio] ELSE 'Nombre no disponible' END) AS Median_n_no2
FROM [TMP].[dbo].[muestras] AS MU
LEFT JOIN [TMP].[dbo].[sitios] AS DS
    ON MU.[clave_sitio] = DS.[clave_sitio]