#!/usr/bin/env python
"""
Script de migración: añade la columna model_meta JSONB a measurements.
Uso manual: python tools/migrate_add_model_meta.py
"""
from src.db_connection import conectar_bd

SQL = """
ALTER TABLE measurements
  ADD COLUMN IF NOT EXISTS model_meta JSONB;
"""

if __name__ == '__main__':
    conn = conectar_bd()
    cur = conn.cursor()
    try:
        cur.execute(SQL)
        conn.commit()
        print("✅ model_meta column added (or already existed)")
    except Exception as e:
        print("✗ Error applying migration:", e)
        try:
            conn.rollback()
        except Exception:
            pass
    finally:
        cur.close()
        conn.close()
