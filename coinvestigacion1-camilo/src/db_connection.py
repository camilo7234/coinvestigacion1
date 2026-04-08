"""
Manejo de la conexión y creación de tablas en PostgreSQL,
utilizando la interfaz DB-API de pg8000.
"""

import logging
import pg8000  # utiliza la interfaz DB-API, no pg8000.native
import os
import sys
import ssl

# Logging básico
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def conectar_bd():
    """
    Establece la conexión con la base de datos PostgreSQL en la nube (Neon) usando pg8000 DB-API.
    """
    try:
        conn = pg8000.connect(
            user="neondb_owner",
            password="npg_pgxVl1e3BMqH",
            database="neondb",  # Cambia aquí si quieres conectar a otra BD
            host="ep-lucky-morning-adafnn5y-pooler.c-2.us-east-1.aws.neon.tech",
            port=5432,
            ssl_context=ssl.create_default_context()
        )
        logging.info("✅ Conexión a PostgreSQL en Neon establecida (pg8000 DB-API con SSL).")
        return conn
    except Exception as e:
        logging.error("❌ Error abriendo la conexión a PostgreSQL (Neon): %s", e)
        sys.exit(1)

def create_tables(conn):
    """
    Crea las tablas de la base de datos según el archivo schema.sql.
    Usa la interfaz DB-API de pg8000 (cursor, commit, rollback).
    """
    schema_path = os.path.join(os.path.dirname(__file__), '..', 'schema.sql')
    schema_path = os.path.abspath(schema_path)

    if not os.path.exists(schema_path):
        logging.error("❌ No se encontró schema.sql en: %s", schema_path)
        sys.exit(1)

    cur = conn.cursor()
    try:
        with open(schema_path, 'r', encoding='utf-8') as f:
            sql = f.read()
        cur.execute(sql)
        conn.commit()
        logging.info("✅ Tablas creadas (o ya existían) con éxito.")
    except Exception as e:
        logging.error("❌ Error creando tablas: %s", e)
        try:
            conn.rollback()
        except Exception:
            pass
        sys.exit(1)
    finally:
        cur.close()

if __name__ == "__main__":
    conn = conectar_bd()
    create_tables(conn)
    conn.close()
