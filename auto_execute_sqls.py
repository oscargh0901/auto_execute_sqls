import json
import os
import re
import pyodbc

def main():
    with open("config.json") as archivo_config:
        config = json.load(archivo_config)

    ruta = config["ruta"]
    conexion_str = config["conexion"]

    conexion = pyodbc.connect(conexion_str)
    
    archivos_sql = obtener_archivos_sql(ruta)
    for archivo_sql in archivos_sql:
        ejecutar_sql(archivo_sql, ruta, conexion)
    
    conexion.close()
    print("Se han aplicado todos los archivos SQL.")

def obtener_archivos_sql(ruta):
    archivos_sql = [archivo for archivo in os.listdir(ruta) if archivo.endswith(".sql") and re.match(r"\d+\.-", archivo)]
    archivos_sql.sort(key=lambda x: int(re.search(r"\d+", x).group()))  # Ordenar por el número delante del guion
    return archivos_sql

def ejecutar_sql(archivo_sql, ruta, conexion):
    nombre_archivo = archivo_sql.split("-", 1)[1].strip()

    with open(os.path.join(ruta, archivo_sql), "r") as archivo:
        sql = archivo.read()

        try:
            cursor = conexion.cursor()
            cursor.execute(sql)
            conexion.commit()
            print(f"Se ha aplicado el archivo SQL {nombre_archivo}.")
        except pyodbc.Error as error:
            print(f"Error al ejecutar el archivo SQL {nombre_archivo}: {error}")

if __name__ == "__main__":
    main()
