#!/usr/bin/env python
import os
import sys
import logging

# 1. Configuración básica de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# 2. Agregar ruta al SDK de PalmSens (igual que en pstrace_session.py)
sdk_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'sdk', 'PSPythonSDK', 'pspython'))
if sdk_path not in sys.path:
    sys.path.append(sdk_path)
    logging.info("Ruta del SDK añadida: %s", sdk_path)

# 3. Importar módulo para carga de sesiones y extracción de datos
from src.pstrace_session import get_session_dict

# 4. Importar funciones de base de datos
from src.db_connection import conectar_bd, create_tables
from src.insert_data import insert_session

if __name__ == "__main__":
    # --- Paso A: Crear/conectarse a la base de datos y tablas ---
    conn = conectar_bd()               # Conecta a deteccion_metales en PostgreSQL
    #create_tables(conn)                # Crea las tablas si no existen

    # --- Paso B: Leer y procesar el archivo .pssession ---
    logging.info("Obteniendo diccionario de sesión desde .pssession...")
    sesion_dict = get_session_dict()   # Devuelve el dict con todas las mediciones

    # --- Paso C: Insertar los datos en la base de datos ---
    logging.info("Insertando datos de sesión en la base de datos...")
    insert_session(conn, sesion_dict, "ultima_medicion.pssession")

    logging.info("Todos los datos se han insertado correctamente en PostgreSQL.")

    # --- Paso D: Cerrar conexión ---
    conn.close()
    logging.info("Conexión cerrada. Proceso completado.")
