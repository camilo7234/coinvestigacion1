"""
Script de diagnóstico para inspeccionar la estructura devuelta por las funciones de `pstrace_session.py`.
Usa las funciones normalizadas del módulo `src/pstrace_session.py` para cargar y procesar una .pssession y
muestra un resumen compacto (keys, tamaños, min/max, ejemplos).

Uso: desde la raíz del repo:
    python tools/diagnose_pssession.py data/ultima_medicion.pssession

El script imprime un resumen y no emite eventos.
"""
import sys
import os
from pprint import pprint

REPO_ROOT = os.path.dirname(os.path.dirname(__file__))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

import src.pstrace_session as pstrace


def summarize(obj, prefix=""):
    """Return a short summary for common types."""
    t = type(obj)
    if obj is None:
        return ("None", None)
    if isinstance(obj, dict):
        return ("dict", {k: summarize(v) for k, v in list(obj.items())[:10]})
    if isinstance(obj, (list, tuple)):
        length = len(obj)
        sample = obj[:3]
        return (f"{t.__name__}[{length}]", [summarize(x) for x in sample])
    # numpy arrays
    try:
        import numpy as np
        if isinstance(obj, np.ndarray):
            return (f"ndarray shape={obj.shape}", {"min": float(obj.min()), "max": float(obj.max())})
    except Exception:
        pass
    # primitives
    if isinstance(obj, (int, float, str, bool)):
        return (t.__name__, obj)
    # fallback: repr short
    s = repr(obj)
    return (t.__name__, s[:200])


def main():
    if len(sys.argv) < 2:
        print("Uso: python tools/diagnose_pssession.py <ruta_a_pssession>")
        sys.exit(2)

    path = sys.argv[1]
    if not os.path.exists(path):
        print(f"Archivo no encontrado: {path}")
        sys.exit(1)

    print("Cargando módulo pstrace_session y ejecutando flujo normalizado...\n")

    # Intentamos usar las funciones normalizadas que mencionaste.
    # No asumimos la implementación interna: probamos llamadas seguras con try/except.

    # Intento seguro: construir método LoadSessionFile y llamar a cargar_sesion_pssession
    sess = None
    try:
        dll = None
        try:
            dll = pstrace.configurar_sdk_palmsens()
            metodo = pstrace.cargar_y_configurar_metodo_load(dll)
            sess = pstrace.cargar_sesion_pssession(metodo, path)
            print("-> cargar_sesion_pssession: OK")
        except SystemExit as se:
            # Las funciones de pstrace_session pueden hacer sys.exit en fallos críticos;
            # capturamos y seguimos con otras rutas de diagnóstico.
            print("-> cargar_sesion_pssession: SKIPPED por SystemExit interno (SDK/DLL faltante o crítico)")
            sess = None
        except Exception as e:
            print("-> cargar_sesion_pssession: FALLÓ, excepción:")
            print(e)
            sess = None
    except Exception as e:
        print("-> cargar_sesion_pssession: FALLÓ en bloque externo:")
        print(e)
        sess = None

    # extraer y procesar
    processed = None
    # Cargar límites PPM y llamar correctamente a extraer_y_procesar_sesion_completa
    processed = None
    try:
        limites = pstrace.cargar_limites_ppm()
    except Exception as e:
        print("-> cargar_limites_ppm: FALLÓ, usando límites por defecto:", e)
        limites = {}

    try:
        processed = pstrace.extraer_y_procesar_sesion_completa(path, limites)
        print("-> extraer_y_procesar_sesion_completa: OK")
    except Exception as e:
        print("-> extraer_y_procesar_sesion_completa: FALLÓ, excepción:")
        print(e)

    # intentar calcular estimaciones ppm si existe
    ppm = None
    try:
        if processed is not None and hasattr(pstrace, "calcular_estimaciones_ppm"):
            # calcular_estimaciones_ppm espera (datos_pca, limites_ppm) — extraemos pca_scores de processed
            try:
                # Si processed es el resultado completo, tomar pca de la primera medición
                if isinstance(processed, dict) and processed.get('measurements'):
                    first = processed['measurements'][0]
                    datos_pca = first.get('pca_scores') or first.get('pca_data') or []
                else:
                    datos_pca = processed

                ppm = pstrace.calcular_estimaciones_ppm(datos_pca, limites)
                print("-> calcular_estimaciones_ppm: OK")
            except Exception as e:
                print("-> calcular_estimaciones_ppm: FALLÓ en llamada interna:")
                print(e)
        else:
            print("-> calcular_estimaciones_ppm: SKIP (no hay processed o función)")
    except Exception as e:
        print("-> calcular_estimaciones_ppm: FALLÓ, excepción:")
        print(e)

    # Intento adicional: extract_session_dict ofrece una vista simplificada (usa límites internamente)
    gui_summary = None
    try:
        if hasattr(pstrace, 'extract_session_dict'):
            gui_summary = pstrace.extract_session_dict(path)
            print("-> extract_session_dict: OK")
        else:
            print("-> extract_session_dict: SKIP (no definida)")
    except Exception as e:
        print("-> extract_session_dict: FALLÓ, excepción:")
        print(e)

    print('\nResumen de `sess` (resultado de cargar_sesion_pssession):')
    if sess is None:
        print('  <None>')
    else:
        try:
            pprint({
                'type': type(sess).__name__,
                'repr': repr(sess)[:500]
            })
        except Exception as e:
            print('  (no se puede repr sess)', e)

    print('\nResumen de `processed` (extraer_y_procesar_sesion_completa):')
    if processed is None:
        print('  <None>')
    else:
        # if it's a dict, print keys and short summary per key
        if isinstance(processed, dict):
            print('  keys:', list(processed.keys()))
            for k in processed.keys():
                try:
                    v = processed[k]
                    print(f"\n  key: {k}")
                    pprint(summarize(v, prefix=k))
                except Exception as e:
                    print(f"   (error describiendo key {k}: {e})")
        else:
            print('  processed type:', type(processed))
            print('  summary:')
            pprint(summarize(processed))

    print('\nResumen de `ppm` (estimaciones):')
    if ppm is None:
        print('  <None>')
    else:
        try:
            pprint(summarize(ppm))
        except Exception as e:
            print('  (no se puede repr ppm)', e)

    print('\nDiagnóstico completado.')


if __name__ == '__main__':
    main()
