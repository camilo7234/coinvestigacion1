"""
TEST DE CONEXIÓN IOT CON PALMSENS
---------------------------------
Este script valida:
1. Descubrimiento de dispositivos
2. Conexión al instrumento
3. Estado del instrumento
4. Desconexión segura
"""

import sys, os
# 🔧 Aseguramos que Python vea la carpeta src como raíz
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from pstrace_connection import (
    descubrir_instrumentos,
    conectar_instrumento,
    estado_instrumento,
    desconectar_instrumento,
    PalmSensConnectionError
)

# Importamos la función real de pstrace_session con su nombre correcto
from pstrace_session import cargar_sesion_pssession


def main():
    print("=== TEST CONEXIÓN IOT ===")

    try:
        # 1) Descubrir dispositivos
        dispositivos = descubrir_instrumentos()
        print("Dispositivos encontrados:", dispositivos)

        if not dispositivos:
            print("⚠ No se detectó ningún dispositivo. Conéctalo y vuelve a probar.")
            return

        # 2) Conectar al primer dispositivo
        instr = conectar_instrumento()
        print("✓ Conexión establecida")

        # 3) Consultar estado
        est = estado_instrumento(instr)
        print("Estado del instrumento:", est)

        # 4) Desconectar
        desconectar_instrumento(instr)
        print("✓ Desconexión correcta")

    except PalmSensConnectionError as e:
        print("✗ Error de conexión:", e)
    except Exception as e:
        print("✗ Error inesperado:", e)


if __name__ == "__main__":
    main()