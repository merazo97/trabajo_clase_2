import psycopg2
import json
import nbformat
import pandas as pd
from datetime import datetime

# Paso 1: Crear la base de datos y la tabla desde Python
def create_database_and_table():
    try:
        # Conexión al servidor PostgreSQL (sin especificar una base de datos)
        conn = psycopg2.connect(
            user="postgres",  # Usuario predeterminado
            password="postgres",
            host="localhost",
            port="5432"
        )
        conn.autocommit = True  # Necesario para crear una base de datos
        cursor = conn.cursor()

        # Crear la base de datos "Grupo4_data" si no existe
        cursor.execute("SELECT datname FROM pg_database WHERE datname='Grupo4_data';")
        if not cursor.fetchone():
            cursor.execute("CREATE DATABASE Grupo4_data;")
            print("Base de datos 'Grupo4_data' creada correctamente.")
        else:
            print("La base de datos 'Grupo4_data' ya existe.")

        # Cerrar la conexión inicial
        cursor.close()
        conn.close()

        # Conectar a la nueva base de datos
        conn = psycopg2.connect(
            database="Grupo4_data",
            user="postgres",
            password="postgres",
            host="localhost",
            port="5432"
        )
        cursor = conn.cursor()

        # Crear la tabla "structured_logs" si no existe
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS structured_logs (
            session_id,
            network_packet_size,
            protocol_type,
            login_attempts,
            session_duration
        );
        """)
        print("Tabla 'structured_logs' creada correctamente.")

        # Confirmar cambios y cerrar la conexión
        conn.commit()
        cursor.close()
        conn.close()

    except Exception as e:
        print(f"Error al crear la base de datos o la tabla: {e}")

# Paso 2: Insertar datos estructurados en la tabla
def insert_structured_logs():
    try:
        # Conectar a la base de datos
        conn = psycopg2.connect(
            database="Grupo4_data",
            user="postgres",
            password="postgres",
            host="localhost",
            port="5432"
        )
        cursor = conn.cursor()

        # Datos de ejemplo para insertar
        logs = [
        # Ruta al archivo Jupyter Notebook
        ruta_archivo = '/workspaces/trabajo_clase_2/Grupo4_trabajo2.ipynb'

        # Leer el archivo .ipynb
        with open(ruta_archivo, 'r', encoding='utf-8') as f:
            notebook = nbformat.read(f, as_version=4)

        # Extraer las celdas del notebook
        celdas = notebook['cells']

        # Filtrar las celdas que contienen codigo
        celdas_de_codigo = [celda for celda in celdas if celda['cell_type'] == 'code']

        # Inicializar un DataFrame vacio
        df_final = pd.DataFrame()

        # Ejecutar cada celda de codigo y capturar los resultados en un DataFrame
        for celda in celdas_de_codigo:
            codigo = celda['source']
            try:
                exec(codigo)
            except Exception as e:
                print(f"Error al ejecutar el codigo: {e}")

        # Extraer las ultimas 5 columnas del DataFrame
        if not df_final.empty:
            ultimas_columnas = df_final.iloc[:, -5:]
            print(ultimas_columnas)
        else:
            print("No se encontraron DataFrames en el archivo.")

        # Nota: Asegurate de que las celdas de codigo contienen la creacion del DataFrame `df_final`

        ]

        # Insertar registros en la tabla
        cursor.executemany("""
        INSERT INTO structured_logs (session_id,network_packet_size,protocol_type,login_attempts,session_duration)
        VALUES (%s, %s, %s, %s);
        """, logs)

        # Confirmar cambios y cerrar la conexión
        conn.commit()
        print("Datos estructurados insertados correctamente.")
        cursor.close()
        conn.close()

    except Exception as e:
        print(f"Error al insertar datos estructurados: {e}")

# Paso 3: Guardar logs no estructurados en un archivo JSON
def save_unstructured_logs():
    try:
        # Simulación de logs no estructurados
        unstructured_logs = [
            "[2025-02-22 10:10:00] Firewall Alert: Blocked incoming traffic from 10.0.0.1 to port 22.",
            "[2025-02-22 10:15:00] IDS Alert: Suspicious activity detected from IP 192.168.1.20."
        ]

        # Guardar logs en un archivo JSON
        with open("unstructured_logs.json", "w") as file:
            json.dump(unstructured_logs, file, indent=4)

        print("Logs no estructurados guardados en 'unstructured_logs.json'")

    except Exception as e:
        print(f"Error al guardar logs no estructurados: {e}")

# Ejecutar todas las funciones
if __name__ == "__main__":
    create_database_and_table()  # Crear base de datos y tabla
    insert_structured_logs()     # Insertar datos estructurados
    save_unstructured_logs()     # Guardar logs no estructurados
