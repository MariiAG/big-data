# --------------------------------------------------------------------------
# INICIO PUNTO 6 / LIMPIEZA DE DATOS
# --------------------------------------------------------------------------

import time
import sys
import re
import random
import pandas as pd
import psycopg2
from   psycopg2 import Error
import csv

# Variables globales
error_con = False
hola = "Hola mundo! Equipo 6"

# Parámetros de conexión de la Base de datos local
v_host	   = "localhost"
v_port	   = "5432"
v_database = "bigdata"
v_user	   = "postgres"
v_password = "postgres"

def procesar_fechas(fecha):
    fecha_str = str(fecha)

    # Se presenta caso DD-MM-YY (se asume siglo actual, es decir, el 2000 y por eso se agrega el 20 para formar los 4 digitos del año) y se intercambian las partes del string para formar 
    if '-' in fecha_str and len(fecha_str) == 8:
        elementos = fecha_str.split('-')
        return f"20{elementos[2]}-{elementos[1]}-{elementos[0]}"

    # Se presenta caso de formato correcto YYYYMMDD solo se le agregan los guiones medios (solo se elije entre que posiciones de caracteres esta cada valor)
    elif len(fecha_str) == 8 and fecha_str.isdigit():
        # mes_o_año_inicio = int(fecha_str[:4])
        # mes_o_año_final = int(fecha_str[4:])
        # if mes_o_año_inicio >= 2000:
        #     return f"{fecha_str[:4]}-{fecha_str[4:6]}-{fecha_str[6:]}"
        # elif mes_o_año_final >= 2000:
        #     return f"{fecha_str[4:]}-{fecha_str[2:4]}-{fecha_str[:2]}"     
        año_inicio = int(fecha_str[4:6])
        if año_inicio < 12:
            return f"{fecha_str[:4]}-{fecha_str[4:6]}-{fecha_str[6:]}"
        else:
            return f"{fecha_str[4:]}-{fecha_str[2:4]}-{fecha_str[:2]}"     
    
    # Se presenta caso AAMMDD (de nuevo se asume que el registro es de este siglo) y simplemente se le agregan los giones como el anterior ejemplo
    elif len(fecha_str) == 6 and fecha_str.isdigit():
        mes_o_año = int(fecha_str[:2])
        if mes_o_año <= 12:
            return f"20{fecha_str[:2]}-{fecha_str[2:4]}-{fecha_str[4:]}"
        else: 
            return f"20{fecha_str[4:]}-{fecha_str[2:4]}-{fecha_str[:2]}"
    # Se presenta caso DD-MM-YYYY y en este caso solo es ordenar los valores partiendo de sus separacion por - y se escriben de manera inversa
    elif '-' in fecha_str and len(fecha_str) == 10:
        elementos = fecha_str.split('-')
        return f"{elementos[2]}-{elementos[1]}-{elementos[0]}"
    return fecha

# --------------------------------------------------------------------------
# CONEXIÓN A LA BASE DE DATOS
# --------------------------------------------------------------------------
try:
    ## CONEXION
    connection = psycopg2.connect(user= v_user, password=v_password, host= v_host, port= v_port, database= v_database)
    cursor = connection.cursor()
    cursor.execute("SELECT version();")
    record = cursor.fetchone()
    print("PostgreSQL Información del Servidor")
    print(connection.get_dsn_parameters(), "\n")    
    print("Python version: ",sys.version)    
    print("Estás conectado a - ", record, "\n")
    print("Base de datos:", v_database, "\n")
except (Exception, Error) as error:
    print("Error: ", error)
    error_con = True
finally:
    if (error_con):            
        sys.exit("Error de conexión con servidor PostgreSQL")


# --------------------------------------------------------------------------
# ARREGLO DE FECHAS
# --------------------------------------------------------------------------
try:
    print("ARREGLO DE FECHAS")  
    print("Registro de fechas incorrectas")  
    sql = """SELECT id_registro, fecha FROM operaciones WHERE fecha !~ '^\d{4}-\d{2}-\d{2}$';"""
    cursor.execute(sql)
    registros = cursor.fetchall()
    df = pd.DataFrame(data=registros, columns=['id_registro', 'fecha'])
    print(df)
    df['fecha'] = df['fecha'].apply(procesar_fechas) # aplica la funcion de validacion para los casos especificos encontrados en la base de datos
    print("Registros corregidos")
    print(df)
    for index, row in df.iterrows():
        sql = """UPDATE public.operaciones SET fecha = %s WHERE id_registro = %s"""
        cursor.execute(sql, (row['fecha'], row['id_registro']))
    connection.commit()
except (Exception, Error) as error:
    print("Error de procesamiento de la tabla!", error)
finally:
    if (connection):
        connection.close()
        print("Conexión PostgreSQL cerrada")    
        
print("Fin del proceso")
# Fin del algoritmo
