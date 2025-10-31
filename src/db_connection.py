import pg8000
import json

DB_CONFIG = {
    "host": "ep-lucky-morning-adafnn5y-pooler.c-2.us-east-1.aws.neon.tech",
    "user": "neondb_owner", 
    "password": "npg_pgxVl1e3BMqH",
    "database": "neondb",
    "port": 5432
}

try:
    conn = pg8000.connect(**DB_CONFIG)
    cur = conn.cursor()
    
    print("=== DIAGNÓSTICO SESIÓN 141 ===")
    cur.execute("""
        SELECT id, title, contamination_level, clasificacion,
               ppm_estimations, pg_typeof(ppm_estimations) as tipo_ppm
        FROM measurements 
        WHERE session_id = 141
        ORDER BY id
    """)
    
    rows = cur.fetchall()
    print(f"Mediciones encontradas para sesión 141: {len(rows)}")
    
    for row in rows:
        mid, title, cont_level, clasif, ppm_est, tipo = row
        print(f"\n--- MEDICIÓN {mid} ---")
        print(f"Title: {title}")
        print(f"Contamination level: {cont_level}")
        print(f"Clasificacion: {clasif}")
        print(f"PPM estimations tipo: {tipo}")
        print(f"PPM estimations valor: {ppm_est}")
        
        if ppm_est:
            try:
                if isinstance(ppm_est, str):
                    ppm_dict = json.loads(ppm_est)
                else:
                    ppm_dict = ppm_est
                
                print("  Valores PPM individuales:")
                max_val = 0
                max_metal = ""
                for metal, valor in ppm_dict.items():
                    print(f"    {metal}: {valor} (tipo: {type(valor)})")
                    if isinstance(valor, (int, float)) and valor > max_val:
                        max_val = valor
                        max_metal = metal
                
                print(f"  MAX PPM calculado: {max_val} ({max_metal})")
            except Exception as e:
                print(f"  Error procesando PPM: {e}")
    
    conn.close()
    
except Exception as e:
    print(f"Error: {e}")