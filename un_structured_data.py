import psycopg2
import json
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

        # Crear la base de datos "kaggle_logs" si no existe
        cursor.execute("SELECT datname FROM pg_database WHERE datname='kaggle_logs';")
        if not cursor.fetchone():
            cursor.execute("CREATE DATABASE kaggle_logs;")
            print("Base de datos 'kaggle_logs' creada correctamente.")
        else:
            print("La base de datos 'kaggle_logs' ya existe.")

        # Cerrar la conexión inicial
        cursor.close()
        conn.close()

        # Conectar a la nueva base de datos
        conn = psycopg2.connect(
            database="kaggle_logs",
            user="postgres",
            password="postgres",
            host="localhost",
            port="5432"
        )
        cursor = conn.cursor()

        # Crear la tabla "structured_logs" si no existe
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS structured_logs (
            id VARCHAR(50),
            network_packet INT,
            protocol VARCHAR(10),
            ip_address INT,
            timestamp DECIMAL(15, 6)
            
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
            database="kaggle_logs",
            user="postgres",
            password="postgres",
            host="localhost",
            port="5432"
        )
        cursor = conn.cursor()

        # Datos de ejemplo para insertar
        logs = [
            ("SID_00001", 599, "TCP", 4, "492.983263"), 
            ("SID_00002", 472, "TCP", 3, "1557.996461"), 
            ("SID_00003", 629, "TCP", 3, "75.044262"), 
            ("SID_00004", 804, "UDP", 4, "601.248835"), 
            ("SID_00005", 453, "TCP", 5, "532.540888"), 
            ("SID_00006", 453, "UDP", 5, "380.471550"), 
            ("SID_00007", 815, "ICMP", 4, "728.107165"), 
            ("SID_00008", 653, "TCP", 3, "12.599906"), 
            ("SID_00009", 406, "TCP", 2, "542.558895"), 
            ("SID_00010", 608, "UDP", 6, "531.944107")
        ]

        # Insertar registros en la tabla
        cursor.executemany("""
        INSERT INTO structured_logs (id, network_packet, protocol, ip_address, timestamp)
        VALUES (%s, %s, %s, %s, %s);
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
