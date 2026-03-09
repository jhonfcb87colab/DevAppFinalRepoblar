import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import sqlite3


# se crea una base de datos en SQLite llamada veterinaria.db
conn = sqlite3.connect('C:/ARCHIVOS_DEV/Archivos_Py/PROY_INTERFACES_PY/Dev_Repoblamiento/Dataset/Credenciales.db')
# Creamos un cursor para ejecutar comandos SQL
cursor = conn.cursor()

def Funcion_ejecutar_DDL(query_ddl, conexion=conn):
    """
    Funcion encargada de ejecutar  sentencias  de tipo DDL(Definición de Datos - Define o modifica la estructura de la base de datos - CREATE, ALTER, DROP) 
    """
    cursor = conexion.cursor()
    cursor.execute(query_ddl)
    conexion.commit()
    return f"DDL ejecutado correctamente: {query_ddl}"

# Definimos una Función Auxiliar encargada de ejecutar consultas SQL
# Al llamar la consulta se recivira una tabla  en un DataFrame de pandas
def Funcion_ejecutar_DQL(query_dql, conexion=conn):
  """
  Funcion encargada de ejecutar  sentencias  de tipo DQL(Consulta de Datos - Recupera información - SELECT) 
  Encargada de devuer el resultado en un dataframe de pandas 
  """
  df = pd.read_sql_query(query_dql, conexion)
  return df

def Funcion_ejecutar_DML(query_dml, conexion=conn):
    """
    Funcion encargada de ejecutar  sentencias  de tipo DML(Manipulación de Datos - Manipula los datos dentro de las tablas - INSERT, UPDATE, DELETE) 
    """
    try:
        cursor = conexion.cursor()
        cursor.execute(query_dml)
        conexion.commit()
    except sqlite3.OperationalError as e:
        print(f" Se genera error  de ejecucion posiblemente  por que la columna ya existe {e}.")

    return f"DML ejecutado correctamente: {query_dml}"




script_crear_tabla_credenciales = """
CREATE TABLE IF NOT EXISTS CREDENCIALES (
                ID INTEGER PRIMARY KEY AUTOINCREMENT,
                TIPO_PERMISO_ACCES VARCHAR(150) NOT NULL,
                ID_ROLE INTEGER NOT NULL,
                LINK TEXT NOT NULL,
                USUARIO VARCHAR(500) UNIQUE NOT NULL,
                PASSWORD TEXT NOT NULL,
                TIPO VARCHAR(50) NULL,
                ESTADO INTEGER DEFAULT 1 -- 1: Habilitado, 0: Deshabilitado
                );
"""

# Ejecución de DDL (Definición)
Funcion_ejecutar_DDL('DROP TABLE IF EXISTS CREDENCIALES')
Funcion_ejecutar_DDL(script_crear_tabla_credenciales)

# Script de Inserción (Corregido el nombre de la tabla)
insertar_usuarios = """
INSERT INTO CREDENCIALES (TIPO_PERMISO_ACCES, ID_ROLE, LINK, USUARIO, PASSWORD, TIPO,ESTADO) 
VALUES ('ADMINISTRADOR', 1, 'NA', 'admin', 'admin123', 'ADMIN', 1);
"""

# Ejecución de DML (Manipulación)
Funcion_ejecutar_DML(insertar_usuarios)