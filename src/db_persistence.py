#!/usr/bin/env python
"""
db_persistence.py
Funciones para guardar sesiones y mediciones en PostgreSQL,
usando la conexión de db_connection.py

CORRECCIONES APLICADAS (rama optimizaciones):
  1. classification_group estaba INVERTIDO:
       Original: CONTAMINADA=1, ANOMALA=2
       Correcto:  ANOMALA=1,     CONTAMINADA=2  (según GROUP_MAP en canonical.py)
  2. La clasificación raw se persistía sin normalizar ("ANÓMALA" != "ANOMALA" en BD).
       Ahora se normaliza siempre con normalize_classification() antes de insertar.
  3. Se elimina el bloque if/elif manual y se delega 100% a
       canonical.classification_group_from_label() como fuente única de verdad.
"""
import logging
from db_connection import conectar_bd
from canonical import normalize_classification, classification_group_from_label


def guardar_sesion_y_mediciones(session_info, measurements):
    """
    Inserta una sesión y sus mediciones en la base de datos.

    Traduce la clasificación textual a classification_group (0, 1, 2)
    usando canonical.py como única fuente de verdad:
        0 = SEGURA
        1 = ANOMALA
        2 = CONTAMINADA

    Normaliza la cadena de clasificación antes de persistir para evitar
    inconsistencias por acentos o variantes de mayúsculas ("ANÓMALA" != "ANOMALA").
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

        # 2) Insertar mediciones
        for m in measurements:
            # ---------------------------------------------------------------
            # CORRECCIÓN: normalizar la clasificación raw antes de cualquier
            # operación. Esto convierte "ANÓMALA", "anomala", "Anómala", etc.
            # a la cadena canónica "ANOMALA" que es la que debe ir en la BD.
            # ---------------------------------------------------------------
            clas_raw = m.get("clasificacion", "SEGURA")
            clas = normalize_classification(clas_raw)

            # ---------------------------------------------------------------
            # CORRECCIÓN: delegar el mapeo numérico a canonical.py.
            # El bloque if/elif original estaba INVERTIDO:
            #   CONTAMINADA -> 1  (INCORRECTO, debía ser 2)
            #   ANÓMALA     -> 2  (INCORRECTO, debía ser 1)
            # Ahora usa classification_group_from_label() que lee GROUP_MAP:
            #   SEGURA      -> 0
            #   ANOMALA     -> 1
            #   CONTAMINADA -> 2
            # ---------------------------------------------------------------
            classification_group = classification_group_from_label(clas)

            contamination_level = m.get("contamination_level", 0)
            ppm_estimations = m.get("ppm_estimations") or {}

            logging.debug(
                "Insertando medición -> clas_raw=%s, clas_normalizada=%s, group=%s, "
                "contamination_level=%.2f, ppm=%s",
                clas_raw, clas, classification_group, contamination_level, ppm_estimations
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
                clas,          # <-- se persiste la cadena canónica, nunca el raw
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
