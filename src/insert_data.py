#!/usr/bin/env python
"""
insert_data.py

Módulo autónomo para insertar sesiones y datos electroquímicos
(pca, curvas, puntos) en la base de datos PostgreSQL según
la estructura definida en schema.sql, incluyendo scan_rate.
"""

import os
import sys
import logging
import pg8000
from db_connection import conectar_bd
from pstrace_session import extract_session_dict as extraer_generar, cargar_limites_ppm as cargar_limites

# ——— Bloque 3.1 – Configuración de Logging ———
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# ——— Bloque 3.2 – Guardar sesión con metadatos reales ———
def guardar_sesion(conn, filename, info):
    """
    Inserta en sessions:
      filename, loaded_at,
      scan_rate, start_potential, end_potential, software_version
    """
    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO sessions
              (filename, loaded_at,
               scan_rate, start_potential, end_potential, software_version)
            VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING id
            """,
            (
                filename,
                info.get('loaded_at'),             # Timestamp de carga
                info.get('scan_rate'),             # De .pssession
                info.get('start_potential'),       # De .pssession
                info.get('end_potential'),         # De .pssession
                info.get('software_version')       # De .pssession
            )
        )
        session_id = cur.fetchone()[0]
        conn.commit()
        return session_id
    finally:
        cur.close()


# ——— Bloque 3.3 – Función guardar_mediciones ———
def guardar_mediciones(conn, session_id, measurements):
    """
    Inserta mediciones, curvas y puntos asociados a una sesión.
    Ahora calcula classification_group y contamination_level en base a ppm_estimations y límites.
    """
    cur = conn.cursor()
    try:
        # Cargar límites PPM una sola vez
        limites_ppm = cargar_limites()

        for m in measurements:
            # Determinar classification_group en base a ppm_estimations
            ppm = m.get('ppm_estimations', {})
            excedidos = []
            max_ppm = 0.0

            for metal, valor in ppm.items():
                limite = limites_ppm.get(metal)
                if valor is not None:
                    max_ppm = max(max_ppm, valor)
                    if limite is not None and valor > limite:
                        excedidos.append(metal)

            if excedidos:
                classification_group = 1   # ⚠️ CONTAMINADA
                contamination_level = max_ppm
            else:
                classification_group = 0   # ✅ SEGURA
                contamination_level = max_ppm

            # Insertar fila en measurements con nuevas columnas
            cur.execute(
                """
                INSERT INTO measurements
                  (session_id, title, timestamp, device_serial, curve_count,
                   classification_group, contamination_level)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                RETURNING id
                """,
                (
                    session_id,
                    m['title'],
                    m['timestamp'],
                    m['device_serial'],
                    m['curve_count'],
                    classification_group,
                    contamination_level
                )
            )
            m_id = cur.fetchone()[0]

            # Insertar curvas y puntos de cada medición
            for curve in m.get('curves', []):
                cur.execute(
                    """
                    INSERT INTO curves (measurement_id, curve_index, num_points)
                    VALUES (%s, %s, %s)
                    RETURNING id
                    """,
                    (m_id, curve['index'], len(curve['potentials']))
                )
                curve_id = cur.fetchone()[0]

                data = list(zip(curve['potentials'], curve['currents']))
                cur.executemany(
                    """
                    INSERT INTO points (curve_id, potential, current)
                    VALUES (%s, %s, %s)
                    """,
                    [(curve_id, p, c) for p, c in data]
                )

        conn.commit()
        logging.info("Mediciones, curvas y puntos insertados correctamente con clasificación recalculada.")
    except Exception as e:
        conn.rollback()
        logging.error(f"Error al insertar mediciones: {e}")
        raise
    finally:
        cur.close()

# ——— Bloque 3.4 – Script principal ———
if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Uso: python insert_data.py <ruta_archivo.pssession>")
        sys.exit(1)

    ruta_ps = sys.argv[1]
    # 1) Extraer datos de la sesión
    # Nota: extraer_generar ya carga los límites internamente
    session_data = extraer_generar(ruta_ps)
    if not session_data:
        logging.error("No se pudieron extraer datos de la sesión.")
        sys.exit(1)

    info = session_data['session_info']
    measurements = session_data['measurements']

    # 2) Conectar a la BD y guardar
    conn = conectar_bd()
    try:
        sid = guardar_sesion(
            conn,
            os.path.basename(ruta_ps),
            info
        )
        guardar_mediciones(conn, sid, measurements)
        logging.info(f"Todo insertado con éxito. session_id = {sid}")
    finally:
        conn.close()