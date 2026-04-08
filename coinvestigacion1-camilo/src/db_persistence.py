#!/usr/bin/env python
"""
db_persistence.py

Funciones para guardar sesiones y mediciones en PostgreSQL,
usando la conexión de db_connection.py
"""

import logging
from db_connection import conectar_bd

def guardar_sesion_y_mediciones(session_info, measurements):
    """
    Inserta una sesión y sus mediciones en la base de datos.
    Traduce la clasificación textual a classification_group (0,1,2).
    """

    conn = conectar_bd()
    cur = conn.cursor()

    try:
        # 1) Insertar sesión
        cur.execute("""
            INSERT INTO sessions (filename, loaded_at, scan_rate, start_potential, end_potential, software_version)
            VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING id;
        """, (
            session_info.get('filename'),
            session_info.get('processed_at'),
            session_info.get('scan_rate'),
            session_info.get('start_potential'),
            session_info.get('end_potential'),
            session_info.get('software_version'),
        ))
        session_id = cur.fetchone()[0]
#=========================================================================================
        # 2) Insertar mediciones
#===================================================================================
        for m in measurements:
            clas = m.get("clasificacion", "SEGURA")
            if clas == "CONTAMINADA":
                classification_group = 1
            elif clas == "ANÓMALA":
                classification_group = 2
            else:
                classification_group = 0

            contamination_level = m.get("contamination_level", 0)
            ppm_estimations = m.get("ppm_estimations") or {}

            # Debug detallado antes de insertar
            logging.debug(
                "Insertando medición -> clas=%s, group=%s, contamination_level=%.2f, ppm=%s",
                clas, classification_group, contamination_level, ppm_estimations
            )

            cur.execute("""
                INSERT INTO measurements (
                  session_id, title, timestamp, device_serial, curve_count,
                  pca_scores, ppm_estimations, classification_group,
                  contamination_level, clasificacion
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """, (
                session_id,
                m.get('title', 'Sin título'),
                m.get('timestamp'),
                m.get('device_serial', 'N/A'),
                m.get('curve_count', 0),
                m.get('pca_scores') or [],
                ppm_estimations,
                classification_group,
                contamination_level,
                clas,
            ))

        conn.commit()
        logging.info("✓ Sesión y mediciones guardadas en la BD (id=%s)", session_id)
        return session_id

    except Exception as e:
        conn.rollback()
        logging.error("✗ Error guardando sesión/mediciones: %s", e)
        return None
    finally:
        cur.close()
        conn.close()