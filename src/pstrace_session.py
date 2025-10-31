#!/usr/bin/env python
"""
===================================================================================
PSTRACE SESSION PROCESSOR - VERSIÓN CONSOLIDADA DEFINITIVA
===================================================================================
Autor: Equipo de Investigación
Fecha: Junio 2025
Descripción: Procesador unificado de archivos .pssession de PalmSens con 
             funcionalidades completas de extracción, análisis PCA y generación CSV.

Funcionalidades principales:
- Carga robusta de archivos .pssession
- Procesamiento avanzado de ciclos voltamétricos
- Generación de matrices PCA con promedios de ciclos 2-5
- Estimación de concentraciones PPM
- Exportación CSV estructurada
- Logging detallado y manejo robusto de errores
===================================================================================
"""

import os
import sys
import logging
import json
import datetime
import traceback
import csv
import joblib
import numpy as np
from pathlib import Path
import logging
log = logging.getLogger(__name__)
from canonical import normalize_classification, display_label_from_label

# ===================================================================================
# BLOQUE 1: CONFIGURACIÓN INICIAL CRÍTICA Y DEPENDENCIAS .NET
# ===================================================================================

def configurar_entorno_python_net():
    """Configuración robusta del entorno Python.NET con validación completa"""
    try:
        # Configurar variable de entorno para Python.NET
        os.environ["PYTHONNET_PYDLL"] = r"C:\\coinvestigacion1\\.venv\\Scripts\\python.exe"
        
        # Método 1: Importación directa (pstrace_session original)
        import pythonnet
        pythonnet.load("coreclr")
        
        # Método 2: Importación CLR adicional (insert_data)
        import clr
        
        from System.Reflection import Assembly
        from System import String, Boolean
        
        logging.info("✓ Entorno .NET inicializado correctamente - Modo híbrido")
        return True, Assembly, String, Boolean, clr
        
    except Exception as e:
        logging.critical("✗ Fallo crítico en dependencias .NET: %s", str(e))
        return False, None, None, None, None

# Inicialización temprana del entorno .NET
net_ok, Assembly, String, Boolean, clr = configurar_entorno_python_net()
if not net_ok:
    sys.exit(1)

# ===================================================================================
# BLOQUE 2: CONFIGURACIÓN AVANZADA DE LOGGING
# ===================================================================================

def configurar_logging_avanzado():
    """Sistema de logging robusto con múltiples salidas y formato mejorado"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s',
        handlers=[
            logging.FileHandler("pstrace_debug.log", encoding='utf-8'),
            logging.StreamHandler(sys.stderr)  # stderr para mantener stdout limpio
        ]
    )
    
    logger = logging.getLogger('PalmSensProcessor')
    logger.info("=" * 60)
    logger.info("INICIANDO PSTRACE SESSION PROCESSOR - VERSIÓN CONSOLIDADA")
    logger.info("=" * 60)
    return logger

log = configurar_logging_avanzado()

# ===================================================================================
# BLOQUE 3: GESTIÓN DE LÍMITES PPM Y CONFIGURACIÓN
# ===================================================================================

def cargar_limites_ppm(ppm_file='limits_ppm.json'):
    """
    Carga los límites de concentración PPM desde archivo JSON

    Args:
        ppm_file (str): Ruta al archivo de límites PPM

    Returns:
        dict: Diccionario con factores de conversión PPM
              Ejemplo: {"Cd":0.10, "Zn":3.00, "Cu":1.00, "Cr":0.50, "Ni":0.50}

              Además, para trazabilidad científica se añade una clave interna
              "_limits_version" con metadatos:
                {
                  "sha256": <hex>|None,
                  "mtime": <float_timestamp>|None,
                  "path": <abs_path>,
                  "load_error": <str>|None
                }

              Notas:
                - No se "inventan" valores: cuando falte alguna clave o el valor
                  sea inválido, el metal tendrá valor None y se registrará la
                  razón en los logs. Otras capas del pipeline deben interpretar
                  None como "límite desconocido" y actuar según la política.
    """
    import hashlib
    from pathlib import Path

    # Claves oficiales esperadas
    claves_oficiales = ["Cd", "Zn", "Cu", "Cr", "Ni"]
    limites_por_defecto = {k: None for k in claves_oficiales}

    ppm_path = Path(ppm_file)

    # Metadatos de versión iniciales
    limits_meta = {"sha256": None, "mtime": None, "path": str(ppm_path.resolve()), "load_error": None}

    try:
        if ppm_path.exists():
            # Leer en bytes para calcular hash y luego decodificar para JSON
            with open(ppm_path, 'rb') as f:
                raw = f.read()

            # SHA256 del archivo (trazabilidad)
            try:
                sha256 = hashlib.sha256(raw).hexdigest()
                limits_meta["sha256"] = sha256
            except Exception as e:
                limits_meta["sha256"] = None
                log.warning("⚠ No se pudo calcular sha256 de %s: %s", ppm_file, str(e))

            # mtime
            try:
                limits_meta["mtime"] = ppm_path.stat().st_mtime
            except Exception:
                limits_meta["mtime"] = None

            # Decodificar y parsear JSON con defensiva
            try:
                text = raw.decode('utf-8')
                parsed = json.loads(text)
                if not isinstance(parsed, dict):
                    raise ValueError("JSON no contiene un objeto/dict en raíz")
            except Exception as e:
                limits_meta["load_error"] = f"json_decode_error: {str(e)}"
                log.error("✗ Error decodificando JSON de límites PPM (%s): %s", ppm_file, str(e))
                # Devolver defaults con metadatos indicando error
                resultados = dict(limites_por_defecto)
                resultados["_limits_version"] = limits_meta
                return resultados

            # Normalizar: asegurar que todas las claves existan y validar numéricos
            resultados = {}
            for metal in claves_oficiales:
                raw_val = parsed.get(metal, None)
                if raw_val is None:
                    resultados[metal] = None
                    log.warning("⚠ Límite para %s no encontrado en JSON, asignando None", metal)
                else:
                    try:
                        val = float(raw_val)
                        # No aceptamos límites no positivos
                        if val <= 0.0:
                            resultados[metal] = None
                            log.warning("⚠ Límite para %s no válido (<=0): %s", metal, raw_val)
                        else:
                            resultados[metal] = val
                    except Exception:
                        resultados[metal] = None
                        log.warning("⚠ Límite para %s no numérico: %s", metal, raw_val)

            # Añadir metadatos de versión
            resultados["_limits_version"] = limits_meta

            log.info("✓ Límites PPM cargados desde %s (version=%s)", ppm_file, limits_meta.get("sha256"))
            return resultados

        else:
            # Archivo no existe: devolver defaults y marcar metadatos
            limits_meta["load_error"] = "file_not_found"
            resultados = dict(limites_por_defecto)
            resultados["_limits_version"] = limits_meta
            log.warning("⚠ Archivo %s no encontrado, usando configuración por defecto", ppm_file)
            return resultados

    except Exception as e:
        # Fallback por error inesperado: devolver defaults con nota de error
        limits_meta["load_error"] = f"unexpected_error: {str(e)}"
        resultados = dict(limites_por_defecto)
        resultados["_limits_version"] = limits_meta
        log.error("✗ Error cargando límites PPM: %s", traceback.format_exc())
        return resultados

# ===================================================================================
# BLOQUE 4: CONFIGURACIÓN Y CARGA DEL SDK PALMSENS
# ===================================================================================

def configurar_sdk_palmsens():
    """
    Configuración robusta del SDK PalmSens con validación de rutas y DLLs
    
    Returns:
        str: Ruta a la DLL principal de PalmSens
    """
    # Construir ruta del SDK
    sdk_path = os.path.abspath(os.path.join(
        os.path.dirname(__file__), '..', 'sdk', 'PSPythonSDK', 'pspython'
    ))
    
    # Validar existencia del SDK
    if not os.path.exists(sdk_path):
        log.critical("✗ Ruta SDK PalmSens inválida: %s", sdk_path)
        sys.exit(1)
    
    # Configurar ruta de la DLL
    dll_path = os.path.join(sdk_path, 'PalmSens.Core.Windows.dll')
    if not os.path.exists(dll_path):
        log.critical("✗ DLL PalmSens no encontrada: %s", dll_path)
        sys.exit(1)
    
    # Agregar SDK al path de Python
    sys.path.insert(0, sdk_path)
    
    try:
        # Importar módulos PalmSens
        import pspymethods
        log.info("✓ SDK PalmSens cargado exitosamente desde: %s", sdk_path)
        log.info("✓ DLL encontrada: %s", dll_path)
        return dll_path
        
    except ImportError as e:
        log.critical("✗ Error importando pspymethods: %s", str(e))
        sys.exit(1)

# ===================================================================================
# BLOQUE 5: CONFIGURACIÓN AVANZADA DEL MÉTODO LOADSESSIONFILE
# ===================================================================================

def cargar_y_configurar_metodo_load(dll_path):
    """
    Carga la DLL y configura dinámicamente el método LoadSessionFile
    Combina las mejores prácticas de ambos códigos originales
    
    Args:
        dll_path (str): Ruta a la DLL de PalmSens
        
    Returns:
        object: Método LoadSessionFile configurado
    """
    try:
        # Cargar ensamblado .NET
        assembly = Assembly.LoadFile(dll_path)
        log.info("✓ Ensamblado .NET cargado: %s", dll_path)
        
        # Obtener tipo de la clase Helper
        tipo = assembly.GetType('PalmSens.Windows.LoadSaveHelperFunctions')
        if not tipo:
            log.critical("✗ Clase LoadSaveHelperFunctions no encontrada")
            sys.exit(1)
        
        # Método 1: Búsqueda por parámetros (pstrace_session original)
        for metodo in tipo.GetMethods():
            if metodo.Name == 'LoadSessionFile':
                params = [p.ParameterType.Name for p in metodo.GetParameters()]
                if params in [['String'], ['String', 'Boolean']]:
                    log.info("✓ LoadSessionFile encontrado - Método 1 - Parámetros: %s", params)
                    return metodo
        
        # Método 2: Búsqueda por tipos CLR (insert_data)
        parametros_posibles = [
            [clr.GetClrType(str)],
            [clr.GetClrType(str), clr.GetClrType(bool)]
        ]
        
        for params in parametros_posibles:
            metodo = tipo.GetMethod("LoadSessionFile", params)
            if metodo:
                log.info("✓ LoadSessionFile encontrado - Método 2 - Tipos CLR: %s", params)
                return metodo
        
        raise AttributeError('LoadSessionFile no encontrado con ningún método')
        
    except Exception as e:
        log.critical("✗ Error crítico cargando método .NET: %s", traceback.format_exc())
        sys.exit(1)

# ===================================================================================
# BLOQUE 6: PROCESAMIENTO AVANZADO DE CICLOS VOLTAMÉTRICOS
# ===================================================================================

def procesar_ciclos_voltametricos(curves):
    """
    Procesamiento avanzado de ciclos voltamétricos según especificaciones.
    Nueva metodología: se elimina el ciclo 1 y se toma únicamente el tercer ciclo
    para análisis (ya no se promedian ciclos 2–5).

    Args:
        curves: Array de curvas voltamétricas

    Returns:
        list: Datos del tercer ciclo (corrientes en float) o lista vacía si falla
    """
    try:
        # Importaciones locales por seguridad (no dependen del scope global)
        import traceback

        # Convertir a lista para manejo uniforme
        arr_curves = list(curves)
        total_ciclos = len(arr_curves)

        log.info("📊 Procesando %d ciclos voltamétricos", total_ciclos)

        # Validar cantidad mínima de ciclos
        if total_ciclos < 3:
            log.warning("⚠ Cantidad insuficiente de ciclos: %d (mínimo: 3)", total_ciclos)
            return []

        # Seleccionar únicamente el tercer ciclo (índice 2)
        tercer_ciclo = arr_curves[2]
        log.info("✓ Ciclo seleccionado para análisis: 3")

        # Extraer valores Y (corrientes) del tercer ciclo
        try:
            corrientes = [float(y) for y in tercer_ciclo.GetYValues()]
            log.debug("  Ciclo 3: %d puntos de corriente extraídos", len(corrientes))
        except Exception as e:
            log.error("✗ Error extrayendo datos del ciclo 3: %s", str(e))
            return []

        # Retornar directamente los valores del tercer ciclo
        log.info("✓ Procesamiento completado: %d puntos obtenidos del ciclo 3", len(corrientes))
        return corrientes

    except Exception as e:
        # Manejo de errores global con traceback
        try:
            import traceback as _tb
            log.error("✗ Error en procesamiento de ciclos: %s", _tb.format_exc())
        except Exception:
            log.error("✗ Error en procesamiento de ciclos: %s", str(e))
        return []
    

# ===================================================================================
# BLOQUE 7: ESTIMACIÓN DE CONCENTRACIONES PPM (VERSIÓN NORMA JSON/0639)
# ===================================================================================

def calcular_estimaciones_ppm(datos_pca, limites_ppm):
    """
    Calcula estimaciones de concentración PPM basadas en los límites oficiales
    definidos en el archivo JSON.

    - Usa los valores PCA como indicadores de contaminación
    - Compara contra cada metal definido en limites_ppm
    - Devuelve un diccionario con, por cada metal, un sub-dict:
         { "ppm": float|null, "pct_of_limit": float|null, "note": str|null }
      donde:
        - "ppm" es la estimación en ppm (si es posible obtenerla; por defecto None)
        - "pct_of_limit" es el porcentaje respecto al límite legal (valor usado para
           decisiones/reglas: 120% → CONTAMINADA, 100% → ANÓMALA, 80% → EN ATENCIÓN)
        - "note" contiene información de validación (por ejemplo "missing_limit")
    - Además devuelve claves auxiliares: "clasificacion" (texto), "max_pct" (float),
      y "method" (metodología usada para la estimación).

    Args:
        datos_pca (list): Datos PCA procesados (valores numéricos representativos)
        limites_ppm (dict): Límites legales de metales (ej. {"Cd":0.1,"Zn":3.0,...})

    Returns:
        dict: Estructura:
          {
            "Cd": {"ppm": None, "pct_of_limit": 12.3, "note": None},
            "Zn": {"ppm": None, "pct_of_limit": None, "note": "missing_limit"},
            ...
            "clasificacion": "SEGURA",
            "max_pct": 12.3,
            "method": "pca_peak_vs_limit"
          }
    """
    if not datos_pca:
        log.warning("⚠ No hay datos PCA para calcular PPM")
        return {}

    try:
        # 1. Obtener un valor representativo desde datos_pca (aquí: valor pico)
        try:
            valor_pico = float(max(datos_pca))
        except Exception as e:
            log.error("✗ No se pudo extraer valor_pico de 'datos_pca': %s", e)
            return {}

        log.info("🔎 Valor pico PCA usado para estimación: %.6f", valor_pico)

        # 2. Preparar resultados por metal con formato claro y trazable
        resultados = {}
        clasificacion = "SEGURA"  # valor inicial
        max_superacion_pct = 0.0  # para determinar la clasificación global

        # Lista ordenada de metales (consistente en todo el pipeline)
        metales = ["Cd", "Zn", "Cu", "Cr", "Ni"]

        for metal in metales:
            # Inicializar sub-dict por metal
            resultados[metal] = {"ppm": None, "pct_of_limit": None, "note": None}

            # Intentar extraer límite numérico para el metal
            limite = None
            try:
                if isinstance(limites_ppm, dict):
                    limite = limites_ppm.get(metal)
            except Exception:
                limite = None

            # Validaciones del límite
            if limite is None:
                resultados[metal]["note"] = "missing_limit"
                log.warning("⚠ Límite para %s ausente en limites_ppm", metal)
                continue

            try:
                limite_val = float(limite)
            except Exception:
                resultados[metal]["note"] = "invalid_limit"
                log.warning("⚠ Límite para %s no numérico: %s", metal, limite)
                continue

            if limite_val <= 0.0:
                resultados[metal]["note"] = "invalid_limit_nonpositive"
                log.warning("⚠ Límite para %s no válido (<=0): %s", metal, limite_val)
                continue

            # Calcular porcentaje respecto al límite: (valor_pico / limite) * 100
            try:
                pct = (valor_pico / limite_val) * 100.0
                # Validar numérico
                pct = float(pct)
                if pct != pct or pct in (float("inf"), float("-inf")):
                    raise ValueError("porcentaje inválido")
            except Exception:
                resultados[metal]["note"] = "calc_error"
                log.warning("⚠ Resultado no numérico para %s (valor_pico=%s, limite=%s)", metal, valor_pico, limite_val)
                continue

            # Guardar pct_of_limit y dejar ppm como None (salvo que exista calibración externa)
            resultados[metal]["pct_of_limit"] = pct
            resultados[metal]["ppm"] = None  # No estimamos ppm directo aquí
            resultados[metal]["note"] = None

            log.debug("  %s: %.2f %% del límite (límite=%.6f)", metal, pct, limite_val)

            # Actualizar máximo porcentaje observado
            if pct > max_superacion_pct:
                max_superacion_pct = pct

        # 3. Determinar clasificación global en función del máximo porcentaje (pct_of_limit)
        if max_superacion_pct >= 120.0:
            clasificacion = "CONTAMINADA"
        elif max_superacion_pct >= 100.0:
            clasificacion = "ANÓMALA"
        elif max_superacion_pct >= 80.0:
            clasificacion = "EN ATENCIÓN"
        else:
            clasificacion = "SEGURA"

        # Añadir metadatos auxiliares para trazabilidad
        resultados["clasificacion"] = clasificacion
        resultados["max_pct"] = float(max_superacion_pct)
        resultados["method"] = "pca_peak_vs_limit"

        log.info("🏷 Clasificación global del agua: %s (%.2f%% máx. superación)", clasificacion, max_superacion_pct)

        return resultados

    except Exception:
        log.error("✗ Error calculando estimaciones PPM: %s", traceback.format_exc())
        return {}

# ===================================================================================
# BLOQUE 7.5: SISTEMA DE CLASIFICACIÓN AVANZADO
# ===================================================================================

class WaterClassifier:
    """
    Sistema avanzado de clasificación de muestras de agua basado en análisis PCA
    y técnicas quimiométricas.
    
    Atributos:
        pca (PCA): Modelo PCA configurado para 2 componentes principales
        threshold (float): Umbral de clasificación para contaminación
        confidence_levels (dict): Niveles de confianza para clasificación
    """
    
    def __init__(self, n_components=2, threshold=0.5):
        """
        Inicializa el clasificador con parámetros configurables.
        
        Args:
            n_components (int): Número de componentes PCA a utilizar
            threshold (float): Umbral para clasificación de contaminación
        """
        try:
            from sklearn.decomposition import PCA
            import numpy as np
            
            self.pca = PCA(n_components=n_components)
            self.threshold = threshold
            self.np = np  # Guardar referencia a numpy
            
            self.confidence_levels = {
                "ALTA": 0.85,
                "MEDIA": 0.65,
                "BAJA": 0.50
            }
            
            log.info("✓ Clasificador inicializado: componentes=%d, umbral=%.2f",
                    n_components, threshold)
            
        except ImportError as e:
            log.error("✗ Error importando dependencias del clasificador: %s", str(e))
            raise
    
    def _preprocess_data(self, voltammetric_data):
        """
        Preprocesa los datos voltamétricos para análisis PCA.
        
        Args:
            voltammetric_data (list/array): Datos crudos de voltametría
            
        Returns:
            array: Datos preprocesados y normalizados
        """
        try:
            # Convertir a array numpy si no lo es
            data = self.np.array(voltammetric_data)
            
            # Remover valores nulos o infinitos
            data = self.np.nan_to_num(data)
            
            # Normalización min-max
            if data.size > 0:
                data_min = self.np.min(data)
                data_max = self.np.max(data)
                if data_max > data_min:
                    data = (data - data_min) / (data_max - data_min)
            
            return data.reshape(1, -1)  # Reshape para PCA
            
        except Exception as e:
            log.error("✗ Error en preprocesamiento: %s", str(e))
            return None
    
    def _calculate_confidence(self, pca_result):
        """
        Calcula el nivel de confianza de la clasificación.
        
        Args:
            pca_result (array): Resultado del análisis PCA
            
        Returns:
            str: Nivel de confianza (ALTA/MEDIA/BAJA)
        """
        try:
            # Calcular distancia al umbral
            max_value = self.np.max(self.np.abs(pca_result))
            distance = self.np.abs(max_value - self.threshold)
            
            # Determinar nivel de confianza
            if distance > self.confidence_levels["ALTA"]:
                return "ALTA"
            elif distance > self.confidence_levels["MEDIA"]:
                return "MEDIA"
            else:
                return "BAJA"
                
        except Exception as e:
            log.error("✗ Error calculando confianza: %s", str(e))
            return "DESCONOCIDA"
    
    def classify_sample(self, voltammetric_data):
        """
        Clasifica una muestra de agua usando técnicas quimiométricas.
        
        Args:
            voltammetric_data (list/array): Datos voltamétricos de la muestra
            
        Returns:
            dict: Resultado de clasificación con formato:
                {
                    "classification": str,  # CONTAMINADA/NO CONTAMINADA
                    "confidence": str,      # ALTA/MEDIA/BAJA
                    "pca_scores": list     # Scores PCA como lista
                }
        """
        try:
            # Validar datos de entrada
            if not voltammetric_data or len(voltammetric_data) == 0:
                log.warning("⚠ Datos voltamétricos vacíos")
                return None
            
            # Preprocesamiento
            processed_data = self._preprocess_data(voltammetric_data)
            if processed_data is None:
                return None
            
            # Análisis PCA
            pca_result = self.pca.fit_transform(processed_data)
            max_value = self.np.max(pca_result)
            
            # Clasificación basada en límites oficiales del JSON (si disponibles)
            classification = "NO CONTAMINADA"
            try:
                # Intentar cargar límites oficiales
                with open("limits_ppm.json", "r") as f:
                    limites_ppm = json.load(f)
                
                # Calcular porcentaje de superación máxima respecto a límites
                max_superacion = 0.0
                for metal in ["Cd", "Zn", "Cu", "Cr", "Ni"]:
                    limite = limites_ppm.get(metal)
                    if limite and limite > 0:
                        porcentaje = (max_value / float(limite)) * 100.0
                        if porcentaje > max_superacion:
                            max_superacion = porcentaje
                
                # Determinar clasificación por porcentaje de superación
                if max_superacion >= 120:
                    classification = "CONTAMINADA"
                elif max_superacion >= 100:
                    classification = "CONTAMINADA"  # anómala pero sobre límite legal
                elif max_superacion >= 80:
                    classification = "NO CONTAMINADA"  # en atención pero bajo límite
                else:
                    classification = "NO CONTAMINADA"
                
                log.info("🏷 Clasificación (JSON): %s (máx. superación: %.2f%%)", classification, max_superacion)
            
            except Exception as e:
                # Fallback a umbral estático si no hay JSON o falla cálculo
                classification = "CONTAMINADA" if max_value > self.threshold else "NO CONTAMINADA"
                log.warning("⚠ Uso de umbral estático por fallo en límites JSON: %s", str(e))
            
            # Calcular confianza
            confidence = self._calculate_confidence(pca_result)
            
            resultado = {
                "classification": classification,
                "confidence": confidence,
                "pca_scores": pca_result.tolist()
            }
            
            log.info("✓ Muestra clasificada: %s (confianza: %s)",
                    classification, confidence)
            
            return resultado
            
        except Exception as e:
            log.error("✗ Error en clasificación: %s", traceback.format_exc())
            return None


# ===================================================================================
# BLOQUE 8: CARGA ROBUSTA DE SESIONES .PSSESSION
# ===================================================================================

def cargar_sesion_pssession(metodo_load, ruta_archivo):
    """
    Carga robusta de archivos .pssession con validación completa
    
    Args:
        metodo_load: Método LoadSessionFile configurado
        ruta_archivo (str): Ruta al archivo .pssession
        
    Returns:
        object or None: Objeto sesión cargado o None si falla
    """
    # Validar existencia del archivo
    if not os.path.exists(ruta_archivo):
        log.error("✗ Archivo .pssession no encontrado: %s", ruta_archivo)
        return None
    
    try:
        # Preparar argumentos según número de parámetros del método
        argumentos = [String(ruta_archivo)]
        num_params = metodo_load.GetParameters().Length
        log.debug("🔧 Método LoadSessionFile detectado con %d parámetros", num_params)

        if num_params == 2:
            argumentos.append(Boolean(False))
        
        # Invocar método de carga
        sesion = metodo_load.Invoke(None, argumentos)
        
        if sesion and hasattr(sesion, "Measurements"):
            mediciones = list(sesion.Measurements)
            log.info("✓ Sesión .pssession cargada exitosamente: %s", ruta_archivo)
            log.info("  Mediciones encontradas: %d", len(mediciones))
            return sesion
        else:
            log.error("✗ La sesión se cargó pero está vacía o no tiene mediciones")
            return None
            
    except FileNotFoundError:
        log.error("✗ Archivo no encontrado al intentar cargar: %s", ruta_archivo)
        return None
    except Exception:
        log.error("✗ Error cargando sesión .pssession: %s", traceback.format_exc())
        return None

# ===================================================================================
# BLOQUE 9: GENERACIÓN AVANZADA DE CSV PCA+PPM
# ===================================================================================

def generar_csv_matriz_pca_ppm(resultados_mediciones):
    """
    Genera archivo CSV con matriz PCA y estimaciones PPM
    Implementa formato estructurado según especificaciones

    NOTA (investigación):
      - Este CSV contiene tanto los porcentajes respecto al límite legal
        (ej. 'Cd_pct' = % del límite) como la predicción global del modelo
        ('ppm_modelo') cuando esté disponible. Además se añaden campos de
        trazabilidad del modelo usados en la predicción por medición.
      - Evitamos ambigüedades renombrando explícitamente las columnas.
    
    Args:
        resultados_mediciones (list): Lista de mediciones procesadas
        
    Returns:
        bool: True si se generó exitosamente, False en caso contrario
    """
    if not resultados_mediciones:
        log.warning("⚠ No hay resultados para generar CSV")
        return False
    
    try:
        # Determinar longitud de datos PCA a partir del primer resultado
        primer_resultado = resultados_mediciones[0]
        longitud_pca = len(primer_resultado.get('pca_scores', []))
        
        if longitud_pca == 0:
            log.warning("⚠ No hay datos PCA para generar CSV")
            return False
        
        # Construir encabezados dinámicos de forma explícita y con unidades claras
        encabezados = ['sensor_id', 'measurement_title']
        encabezados += [f'punto_{i+1}' for i in range(longitud_pca)]  # Puntos PCA / corriente del ciclo 3
        
        # Columnas de porcentaje respecto al límite legal (unidad: %)
        encabezados += ['Cd_pct', 'Zn_pct', 'Cu_pct', 'Cr_pct', 'Ni_pct']
        # Columna con la predicción global del modelo (si existe) en ppm (unidad: ppm)
        encabezados += ['ppm_modelo']
        # Nivel de contaminación (máx % detectado) y clasificación textual
        encabezados += ['contamination_level_pct', 'clasificacion']
        
        # Metadatos del modelo por fila (trazabilidad)
        encabezados += [
            'model_version',
            'model_used_n_features',
            'model_used_baseline',
            'model_baseline_source',
            'model_notes'
        ]
        
        # Crear directorio de salida
        directorio_data = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'data'))
        os.makedirs(directorio_data, exist_ok=True)
        
        # Ruta del archivo CSV
        ruta_csv = os.path.join(directorio_data, 'matriz_pca.csv')
        
        # Escribir CSV con codificación UTF-8
        with open(ruta_csv, 'w', newline='', encoding='utf-8') as archivo_csv:
            escritor = csv.writer(archivo_csv)
            
            # Escribir encabezados
            escritor.writerow(encabezados)
            
            registros_escritos = 0
            # Escribir datos de cada medición
            for resultado in resultados_mediciones:
                fila_datos = [
                    resultado.get('sensor_id', 'N/A'),
                    resultado.get('title', 'Sin título')
                ]
                
                # Agregar datos PCA (ciclo 3 ya procesado en Bloque 10)
                datos_pca = resultado.get('pca_scores', []) or []
                
                # Asegurar que cada fila tenga exactamente 'longitud_pca' valores:
                # - si faltan, se rellenan con None (campo vacío en CSV)
                # - si sobran, se truncan (manteniendo consistencia con encabezados)
                if len(datos_pca) < longitud_pca:
                    padding = [None] * (longitud_pca - len(datos_pca))
                    datos_pca_row = list(datos_pca) + padding
                else:
                    datos_pca_row = list(datos_pca[:longitud_pca])
                
                fila_datos.extend(datos_pca_row)
                
                # Agregar estimaciones PPM (diccionario por metal) - se asume que
                # calcular_estimaciones_ppm devuelve porcentajes (pct) por diseño.
                estimaciones_ppm = resultado.get('ppm_estimations', {}) or {}
                def _safe_get_pct(d, k):
                    v = d.get(k)
                    # Si v es dict y contiene 'pct' o 'pct_of_limit', extraerlo
                    if isinstance(v, dict):
                        return v.get('pct_of_limit') or v.get('pct') or None
                    # Si es numérico, devolverlo
                    try:
                        return float(v) if v is not None else None
                    except Exception:
                        return None
                
                fila_datos.append(_safe_get_pct(estimaciones_ppm, 'Cd'))
                fila_datos.append(_safe_get_pct(estimaciones_ppm, 'Zn'))
                fila_datos.append(_safe_get_pct(estimaciones_ppm, 'Cu'))
                fila_datos.append(_safe_get_pct(estimaciones_ppm, 'Cr'))
                fila_datos.append(_safe_get_pct(estimaciones_ppm, 'Ni'))
                
                # Agregar predicción global del modelo en ppm (si existe)
                ppm_modelo = resultado.get('ppm_modelo', None)
                try:
                    ppm_modelo_val = float(ppm_modelo) if ppm_modelo is not None else None
                except Exception:
                    ppm_modelo_val = None
                fila_datos.append(ppm_modelo_val)
                
                # Agregar nivel de contaminación y clasificación global
                # 'contamination_level' en el resultado ya representa % (según Bloque 10)
                contamination_level = resultado.get('contamination_level', None)
                try:
                    contamination_level_val = float(contamination_level) if contamination_level is not None else None
                except Exception:
                    contamination_level_val = None
                fila_datos.append(contamination_level_val)
                fila_datos.append(resultado.get('clasificacion', 'DESCONOCIDA'))
                
                # Agregar metadatos del modelo (si existen)
                model_meta = resultado.get('model_meta', {}) or {}
                fila_datos.append(model_meta.get('model_version'))
                fila_datos.append(model_meta.get('used_n_features'))
                fila_datos.append(bool(model_meta.get('used_baseline')))
                fila_datos.append(model_meta.get('baseline_source'))
                fila_datos.append(model_meta.get('notes'))
                
                # Normalizar valores None a '' para CSV (mejora legibilidad)
                fila_datos_csv = [("" if v is None else v) for v in fila_datos]
                
                escritor.writerow(fila_datos_csv)
                registros_escritos += 1
        
        log.info("✓ CSV matriz PCA+PPM generado exitosamente: %s", ruta_csv)
        log.info("  Registros escritos: %d", registros_escritos)
        log.info("  Columnas PCA: %d, Columnas PCT por metal: %d, plus ppm_modelo y metadatos", longitud_pca, 5)
        
        return True
        
    except Exception as e:
        log.error("✗ Error generando CSV matriz PCA+PPM: %s", traceback.format_exc())
        return False
    

# ===================================================================================
# BLOQUE 9.5: PREDICCIÓN CON MODELO ENTRENADO (train_memory)
# ===================================================================================

def predecir_con_modelo_entrenado(datos_pca):
    """
    Usa los modelos entrenados (scaler, PCA, RidgeCV) para estimar concentración.
    Se espera que los modelos estén en coinvestigacion1-main/models/.

    Robustez y trazabilidad añadidas:
      - Validaciones de existencia de artefactos.
      - Carga segura de meta.pkl (si existe).
      - Alineado de número de features (padding/truncado) con registro en metadata.
      - Búsqueda de baseline en varios orígenes (meta.pkl, baseline.npy en models/).
      - Política conservadora por defecto: SI NO HAY baseline certificada, NO restar
        la media del propio sample (evita introducir sesgos).
      - Fallbacks documentados para el escalado (scaler.mean_/scale_) y para PCA/predicción.
      - Devolución enriquecida con `model_meta` para auditoría científica.

    Args:
        datos_pca (list or np.ndarray): Corrientes (valores Y del ciclo 3)

    Returns:
        dict: {
            'predicciones': [...],
            'ppm_promedio': float|None,
            'model_meta': {
                'model_version': str|None,
                'used_n_features': int,
                'used_baseline': bool,
                'baseline_source': str|None,
                'notes': str|None
            }
        }
    """
    try:
        import numpy as np
        from pathlib import Path
        import joblib
        import traceback

        ROOT = Path(__file__).resolve().parents[1]
        MODELS_DIR = ROOT / "models"

        scaler_path = MODELS_DIR / "scaler.pkl"
        pca_path = MODELS_DIR / "pca.pkl"
        model_path = MODELS_DIR / "model.pkl"
        meta_path = MODELS_DIR / "meta.pkl"

        # === Validar existencia de modelos ===
        if not (scaler_path.exists() and pca_path.exists() and model_path.exists()):
            log.error("✗ Modelos entrenados no encontrados en 'models/'. Ejecuta train_memory.py primero.")
            return {"predicciones": [], "ppm_promedio": None, "model_meta": {
                "model_version": None, "used_n_features": None, "used_baseline": False, "baseline_source": None,
                "notes": "missing_models"
            }}

        # === Cargar modelos con manejo de errores ===
        try:
            scaler = joblib.load(scaler_path)
            pca = joblib.load(pca_path)
            model = joblib.load(model_path)
        except Exception as e:
            log.error("✗ Error cargando artefactos del modelo: %s", str(e))
            log.debug(traceback.format_exc())
            return {"predicciones": [], "ppm_promedio": None, "model_meta": {
                "model_version": None, "used_n_features": None, "used_baseline": False, "baseline_source": None,
                "notes": "load_error"
            }}

        # Cargar metadata si existe
        meta = {}
        if meta_path.exists():
            try:
                meta = joblib.load(meta_path) or {}
            except Exception as e:
                log.warning("⚠ No se pudo cargar 'meta.pkl' correctamente: %s — continuando sin meta", str(e))
                meta = {}

        # === Preparar entrada X ===
        try:
            X = np.array(datos_pca, dtype=float).reshape(1, -1)
        except Exception as e:
            log.error("✗ Datos de entrada inválidos para predicción: %s", str(e))
            return {"predicciones": [], "ppm_promedio": None, "model_meta": {
                "model_version": meta.get("model_version"), "used_n_features": None, "used_baseline": False,
                "baseline_source": None, "notes": "invalid_input"
            }}
        X = np.nan_to_num(X)

        # === Determinar n_features entrenadas (orden de preferencia) ===
        # 1) meta['n_features'] 2) scaler.mean_.shape[0] 3) X.shape[1] (fallback)
        n_features = None
        if isinstance(meta.get("n_features"), int):
            n_features = int(meta.get("n_features"))
        else:
            scaler_mean = getattr(scaler, "mean_", None)
            if scaler_mean is not None:
                try:
                    n_features = int(np.asarray(scaler_mean).ravel().shape[0])
                except Exception:
                    n_features = None

        if n_features is None:
            n_features = X.shape[1]

        model_meta = {
            "model_version": meta.get("model_version") or getattr(model, "version", None),
            "used_n_features": int(n_features),
            "used_baseline": False,
            "baseline_source": None,
            "notes": None
        }

        # === Alineado de longitudes ===
        if X.shape[1] != n_features:
            try:
                if X.shape[1] < n_features:
                    pad_width = n_features - X.shape[1]
                    X = np.pad(X, ((0, 0), (0, pad_width)), 'constant', constant_values=0.0)
                    model_meta["notes"] = (model_meta.get("notes") or "") + f" padded_{pad_width}"
                else:
                    X = X[:, :n_features]
                    model_meta["notes"] = (model_meta.get("notes") or "") + f" truncated_to_{n_features}"
                log.warning("⚠ Ajustando longitud de entrada a %d características (padding/truncado aplicado)", n_features)
            except Exception as e:
                log.error("✗ Error ajustando longitud de entrada: %s", str(e))
                log.debug(traceback.format_exc())
                return {"predicciones": [], "ppm_promedio": None, "model_meta": model_meta}

        # === Normalización y escalado para PCA (política configurable y trazable) ===
        # Usamos la función centralizada `normalize_for_pca` en src/preprocess.py. Esta función
        # aplica (opcional) resta de baseline y luego un método de escalado configurable.
        try:
            # === Buscar baseline certificada en meta o en models/baseline.npy ===
            baseline_vector = None
            baseline_source = None
            for key in ("baseline", "blank_vector", "baseline_vector", "baseline_mean_vector"):
                if key in meta and meta.get(key) is not None:
                    try:
                        bv = np.array(meta.get(key), dtype=float).reshape(1, -1)
                        if bv.shape[1] != n_features:
                            if bv.shape[1] < n_features:
                                pad_w = n_features - bv.shape[1]
                                bv = np.pad(bv, ((0, 0), (0, pad_w)), 'constant', constant_values=0.0)
                            else:
                                bv = bv[:, :n_features]
                        baseline_vector = bv
                        baseline_source = f"meta:{key}"
                        break
                    except Exception:
                        baseline_vector = None
                        baseline_source = None

            if baseline_vector is None:
                candidate = MODELS_DIR / "baseline.npy"
                if candidate.exists():
                    try:
                        bv = np.load(candidate)
                        bv = np.array(bv, dtype=float).reshape(1, -1)
                        if bv.shape[1] != n_features:
                            if bv.shape[1] < n_features:
                                pad_w = n_features - bv.shape[1]
                                bv = np.pad(bv, ((0, 0), (0, pad_w)), 'constant', constant_values=0.0)
                            else:
                                bv = bv[:, :n_features]
                        baseline_vector = bv
                        baseline_source = "models/baseline.npy"
                    except Exception:
                        baseline_vector = None
                        baseline_source = None

            try:
                from preprocess import normalize_for_pca
            except Exception:
                # Intento alternativo si se ejecuta como paquete
                from .preprocess import normalize_for_pca

            # Método de normalización: se puede definir en meta['normalization_method']
            method = meta.get('normalization_method') or 'use_trained_scaler'

            # Intentar normalizar usando el scaler entrenado (política por defecto)
            try:
                X_scaled, norm_meta = normalize_for_pca(X, baseline_vector=baseline_vector, scaler=scaler, method=method)
                # Actualizar metadatos de modelo para trazabilidad
                model_meta['used_baseline'] = bool(norm_meta.get('used_baseline'))
                model_meta['baseline_source'] = baseline_source
                model_meta['notes'] = (model_meta.get('notes') or '') + f" norm_method:{method}"
            except ValueError as ve:
                # Ocurre si method='use_trained_scaler' pero no se proporcionó scaler.
                log.warning("⚠ normalize_for_pca rechazó el método por falta de artefactos: %s", str(ve))
                # Forzamos un método alternativo seguro: zscore por columnas (no inventa artefactos)
                X_scaled, norm_meta = normalize_for_pca(X, baseline_vector=baseline_vector, scaler=None, method='zscore_columns')
                model_meta['used_baseline'] = bool(norm_meta.get('used_baseline'))
                model_meta['baseline_source'] = baseline_source
                model_meta['notes'] = (model_meta.get('notes') or '') + " fallback:zscore_columns"

        except Exception as e:
            log.error("✗ Error en la normalización para PCA: %s", traceback.format_exc())
            return {"predicciones": [], "ppm_promedio": None, "model_meta": model_meta}

        # === Transformación PCA y predicción con modelo ===
        try:
            X_pca = pca.transform(X_scaled)
        except Exception as e:
            log.error("✗ Error en pca.transform: %s", str(e))
            log.debug(traceback.format_exc())
            return {"predicciones": [], "ppm_promedio": None, "model_meta": model_meta}

        try:
            pred = model.predict(X_pca)
        except Exception as e:
            log.error("✗ Error en model.predict: %s", str(e))
            log.debug(traceback.format_exc())
            return {"predicciones": [], "ppm_promedio": None, "model_meta": model_meta}

        # === Validación de salida y consolidación ===
        try:
            pred_arr = np.asarray(pred).flatten()
            ppm_pred = float(np.mean(pred_arr)) if pred_arr.size > 0 else None
            log.info("💧 Predicción completada → ppm promedio: %s", f"{ppm_pred:.4f}" if ppm_pred is not None else "None")
        except Exception as e:
            log.error("✗ Error consolidando predicciones: %s", traceback.format_exc())
            log.debug(traceback.format_exc())
            return {"predicciones": [], "ppm_promimo": None, "model_meta": model_meta}

        # Enriquecer notas finales
        if not model_meta.get("notes"):
            model_meta["notes"] = "ok"
        else:
            model_meta["notes"] = model_meta["notes"].strip()

        return {
            "predicciones": pred_arr.tolist(),
            "ppm_promedio": ppm_pred,
            "model_meta": model_meta
        }

    except Exception:
        log.error("✗ Error en predicción con modelo entrenado: %s", traceback.format_exc())
        return {"predicciones": [], "ppm_promedio": None, "model_meta": {
            "model_version": None, "used_n_features": None, "used_baseline": False, "baseline_source": None,
            "notes": "unexpected_error"
        }}


# ===================================================================================
# BLOQUE 10: PROCESADOR PRINCIPAL DE SESIONES
# ===================================================================================

def extraer_y_procesar_sesion_completa(ruta_archivo, limites_ppm):
    """
    Función principal que orquesta todo el procesamiento de sesiones .pssession.
    Nueva metodología: se elimina cualquier lógica de promediar ciclos y se toma
    únicamente el tercer ciclo para el análisis de contaminación, comparando
    directamente contra los límites regulatorios oficiales cargados desde JSON.

    Args:
        ruta_archivo (str): Ruta al archivo .pssession
        limites_ppm (dict): Límites de conversión PPM

    Returns:
        dict or None: Diccionario completo con session_info y measurements
    """
    log.info("🚀 Iniciando procesamiento completo de sesión: %s", ruta_archivo)

    # Paso 1: Configurar SDK y método de carga
    dll_palmsens = configurar_sdk_palmsens()
    metodo_load = cargar_y_configurar_metodo_load(dll_palmsens)

    # Paso 2: Cargar sesión .pssession
    sesion_cargada = cargar_sesion_pssession(metodo_load, ruta_archivo)
    if not sesion_cargada:
        return None

    # Paso 3: Extraer información general de la sesión
    informacion_sesion = {
        'session_id': None,
        'filename': os.path.basename(ruta_archivo),
        'scan_rate': getattr(sesion_cargada, 'ScanRate', None),
        'start_potential': getattr(sesion_cargada, 'StartPotential', None),
        'end_potential': getattr(sesion_cargada, 'EndPotential', None),
        'total_cycles': len(list(sesion_cargada.Measurements)),
        'software_version': getattr(sesion_cargada, 'Version', None),
        'processed_at': datetime.datetime.now().isoformat()
    }

    log.info("📋 Información de sesión extraída: %d mediciones", informacion_sesion['total_cycles'])

    # Paso 4: Procesar cada medición
    resultados_mediciones = []

    for idx, medicion in enumerate(sesion_cargada.Measurements, 1):
        titulo = getattr(medicion, "Title", f"Medición_{idx}")
        log.info("🔬 Procesando medición %d/%d: %s", idx, informacion_sesion['total_cycles'], titulo)

        try:
            # Extraer información básica de la medición
            try:
                timestamp = datetime.datetime(
                    medicion.TimeStamp.Year, medicion.TimeStamp.Month, medicion.TimeStamp.Day,
                    medicion.TimeStamp.Hour, medicion.TimeStamp.Minute, medicion.TimeStamp.Second
                )
            except Exception:
                timestamp = None
                log.warning("⚠ Timestamp no disponible para medición %d", idx)

            info_medicion = {
                'measurement_index': idx,
                'sensor_id': getattr(medicion, 'SensorId', getattr(medicion, 'SensorID', None)),
                'title': titulo,
                'timestamp': timestamp,
                'device_serial': getattr(medicion, 'DeviceUsedSerial', 'N/A'),
                'curve_count': getattr(medicion, 'nCurves', 0)
            }

            # Obtener array de curvas
            array_curvas = medicion.GetCurveArray()
            if not array_curvas:
                log.warning("⚠ Medición %d no contiene curvas", idx)
                continue

            # Procesar curvas individuales (todas, para visualización)
            curvas_detalladas = []
            for idx_curva, curva in enumerate(array_curvas):
                curva_info = {
                    'index': idx_curva,
                    'potentials': [float(x) for x in curva.GetXValues()],
                    'currents': [float(y) for y in curva.GetYValues()]
                }
                curvas_detalladas.append(curva_info)

            # Procesamiento PCA: ahora solo tercer ciclo
            datos_pca = procesar_ciclos_voltametricos(array_curvas)
            # Predicción con el modelo entrenado
            resultado_modelo = predecir_con_modelo_entrenado(datos_pca)
            ppm_predicho = resultado_modelo.get("ppm_promedio")

            if not datos_pca:
                log.warning("⚠ No se pudo procesar PCA para medición %d", idx)
                continue

            # Calcular estimaciones PPM contra límites oficiales
            estimaciones_ppm = calcular_estimaciones_ppm(datos_pca, limites_ppm)

            # Determinar nivel de contaminación correctamente usando los porcentajes ya calculados
            nivel_contaminacion = 0.0

            # Recorremos metales en orden conocido; soportamos dos formatos:
            #  - estimaciones_ppm[metal] == porcentaje (float)  OR
            #  - estimaciones_ppm[metal] == {"pct_of_limit": porcentaje, ...}
            for metal in ["Cd", "Zn", "Cu", "Cr", "Ni"]:
                pct_val = None
                try:
                    if isinstance(estimaciones_ppm, dict):
                        v = estimaciones_ppm.get(metal)
                        if isinstance(v, dict):
                            # compatibilidad futura: extraer pct_of_limit si está presente
                            pct_val = v.get("pct_of_limit") if "pct_of_limit" in v else v.get("pct")
                        else:
                            # formato histórico: directamente porcentaje numérico
                            pct_val = v
                    else:
                        pct_val = None
                except Exception:
                    pct_val = None

                # Normalizar y validar numérico
                try:
                    if pct_val is not None:
                        pct_val = float(pct_val)
                        # ignorar NaN/Inf
                        if pct_val != pct_val or pct_val in (float("inf"), float("-inf")):
                            raise ValueError("valor no numérico")
                        if pct_val > nivel_contaminacion:
                            nivel_contaminacion = pct_val
                except Exception:
                    # ignorar valores inválidos
                    continue

            # Determinar clasificación textual (canónica) usando el máximo % observado
            if nivel_contaminacion >= 120.0:
                raw_label = "CONTAMINADA"
            elif nivel_contaminacion >= 100.0:
                raw_label = "ANOMALA"
            elif nivel_contaminacion >= 80.0:
                raw_label = "ANOMALA"
            else:
                raw_label = "SEGURA"

            # Normalizar a etiqueta canónica y etiqueta de presentación
            try:
                clasificacion = normalize_classification(raw_label)
                display_label = display_label_from_label(clasificacion)
            except Exception:
                # Fallback conservador
                clasificacion = raw_label
                display_label = raw_label

            # Consolidar información completa de la medición
            info_medicion.update({
                'curves': curvas_detalladas,
                'pca_scores': datos_pca,
                'ppm_estimations': estimaciones_ppm,
                'clasificacion': clasificacion,
                'display_label': display_label,
                'contamination_level': nivel_contaminacion,
                'model_meta': resultado_modelo.get('model_meta', {}),
                'ppm_modelo': ppm_predicho,
                'pca_points_count': len(datos_pca) if datos_pca else 0
            })

            resultados_mediciones.append(info_medicion)
            log.info("  ✓ Medición procesada: %d curvas, %d puntos PCA, Clasificación=%s, Nivel=%.2f%%",
                     len(curvas_detalladas), len(datos_pca) if datos_pca else 0,
                     clasificacion, nivel_contaminacion)

        except Exception as e:
            log.error("  ✗ Error procesando medición %d: %s", idx, str(e))
            continue

    # Paso 5: Generar archivo CSV matriz PCA+PPM
    csv_generado = False
    if resultados_mediciones:
        csv_generado = generar_csv_matriz_pca_ppm(resultados_mediciones)
        if csv_generado:
            log.info("✓ Archivo CSV matriz PCA+PPM generado exitosamente")
        else:
            log.warning("⚠ No se pudo generar el archivo CSV")

    # Paso 6: Consolidar resultado final
    resultado_final = {
        'session_info': informacion_sesion,
        'measurements': resultados_mediciones,
        'processing_summary': {
            'total_measurements': len(resultados_mediciones),
            'successful_pca': sum(1 for m in resultados_mediciones if m.get('pca_scores')),
            'csv_generated': csv_generado
        }
    }

    log.info("🎯 Procesamiento completado exitosamente")
    log.info("  📊 Mediciones totales: %d", len(resultados_mediciones))
    log.info("  🧮 PCA exitosos: %d", resultado_final['processing_summary']['successful_pca'])
    log.info("=" * 60)

    return resultado_final

# ===================================================================================
# BLOQUE 11: FUNCIÓN DE INTERFAZ PARA LA GUI - extract_session_dict
# ===================================================================================

def extract_session_dict(filepath):
    """
    Función de interfaz para la GUI que extrae los datos de un archivo .pssession
    en el formato esperado por el sistema de carga.

    Args:
        filepath (str): Ruta al archivo .pssession

    Returns:
        dict: Diccionario con estructura {
            'session_info': dict,
            'measurements': list[dict]
        }
    """
    log.info("🔍 Invocando extract_session_dict para la GUI")
    try:
        # 1. Cargar límites PPM
        limites_ppm = cargar_limites_ppm()

        # 2. Procesar el archivo completo
        resultado_completo = extraer_y_procesar_sesion_completa(filepath, limites_ppm)

        if not resultado_completo:
            log.error("✗ No se pudo procesar el archivo: %s", filepath)
            return None

        # 3. Extraer solo la información requerida por la GUI
        session_info = resultado_completo.get('session_info', {})
        measurements = []

        for m in resultado_completo.get('measurements', []):
            # Detectar scores de PCA bajo cualquiera de las dos claves
            pca_scores = m.get('pca_scores') or m.get('pca_data') or []

            # Asegurar ppm_estimations como dict con todas las claves
            ppm_estimations = m.get('ppm_estimations') or {}
            ppm_estimations = {
                'Cd': ppm_estimations.get('Cd'),
                'Zn': ppm_estimations.get('Zn'),
                'Cu': ppm_estimations.get('Cu'),
                'Cr': ppm_estimations.get('Cr'),
                'Ni': ppm_estimations.get('Ni')
            }

            # Incluir clasificación y nivel de contaminación
            clasificacion = m.get('clasificacion', 'DESCONOCIDA')
            contamination_level = m.get('contamination_level', None)

            measurements.append({
                'title': m.get('title', 'Sin título'),
                'timestamp': m['timestamp'].isoformat()
                             if isinstance(m.get('timestamp'), datetime.datetime)
                             else m.get('timestamp'),
                'device_serial': m.get('device_serial', 'N/A'),
                'curve_count': m.get('curve_count', 0),
                'pca_scores': pca_scores,
                'ppm_estimations': ppm_estimations,
                'clasificacion': clasificacion,
                'contamination_level': contamination_level
                ,
                'model_meta': m.get('model_meta', {})
            })

        # 4. Retornar estructura simplificada
        return {
            'session_info': {
                'filename':          session_info.get('filename'),
                'loaded_at':         session_info.get('processed_at'),
                'scan_rate':         session_info.get('scan_rate'),
                'start_potential':   session_info.get('start_potential'),
                'end_potential':     session_info.get('end_potential'),
                'software_version':  session_info.get('software_version')
            },
            'measurements': measurements
        }

    except Exception:
        log.error("💥 Error crítico en extract_session_dict: %s", traceback.format_exc())
        return None
    
# ===================================================================================
# BLOQUE 12: INTERFAZ PRINCIPAL Y PUNTO DE ENTRADA
# ===================================================================================

def main():
    """
    Función principal del programa
    Maneja argumentos de línea de comandos y orquesta el procesamiento
    """
    try:
        # Validar argumentos de línea de comandos
        if len(sys.argv) != 2:
            log.error("✗ Uso incorrecto del programa")
            log.info("📖 Uso correcto: python pstrace_session.py <ruta_archivo.pssession>")
            sys.exit(1)
        
        ruta_archivo_sesion = sys.argv[1]
        log.info("🎯 Archivo objetivo: %s", ruta_archivo_sesion)

        # Validar extensión del archivo
        if not ruta_archivo_sesion.lower().endswith(".pssession"):
            log.warning("⚠ El archivo no tiene extensión .pssession (se intentará procesar de todas formas)")

        # Cargar límites PPM
        try:
            limites_ppm = cargar_limites_ppm()
        except Exception:
            log.critical("💥 No se pudieron cargar los límites PPM desde JSON")
            sys.exit(1)
        
        # Procesar sesión completa
        resultado_procesamiento = extraer_y_procesar_sesion_completa(ruta_archivo_sesion, limites_ppm)
        
        if resultado_procesamiento:
            # Guardar en la base de datos
            try:
                from db_persistence import guardar_sesion_y_mediciones
                session_id = guardar_sesion_y_mediciones(
                    resultado_procesamiento['session_info'],
                    resultado_procesamiento['measurements']
                )
                if session_id:
                    log.info("💾 Sesión guardada en la BD con id=%s", session_id)
                else:
                    log.warning("⚠ No se pudo guardar la sesión en la BD")
            except Exception as e:
                log.error("✗ Error al guardar en la BD: %s", e)

            # Salida JSON limpia por stdout
            print(json.dumps(resultado_procesamiento, indent=2, ensure_ascii=False, default=str))
            log.info("✅ Procesamiento exitoso - JSON enviado a stdout")
            sys.exit(0)
        else:
            log.error("❌ Fallo en el procesamiento - No se generaron resultados")
            sys.exit(1)
            
    except KeyboardInterrupt:
        log.warning("⚠ Procesamiento interrumpido por el usuario")
        sys.exit(2)
    except Exception:
        log.critical("💥 Error crítico inesperado: %s", traceback.format_exc())
        sys.exit(3)

# ===================================================================================
# PUNTO DE ENTRADA DEL PROGRAMA
# ===================================================================================

if __name__ == '__main__':
    main()
#!/usr/bin/env python
"""
===================================================================================
PSTRACE SESSION PROCESSOR - VERSIÓN CONSOLIDADA DEFINITIVA
===================================================================================
Autor: Equipo de Investigación
Fecha: Junio 2025
Descripción: Procesador unificado de archivos .pssession de PalmSens con 
             funcionalidades completas de extracción, análisis PCA y generación CSV.

Funcionalidades principales:
- Carga robusta de archivos .pssession
- Procesamiento avanzado de ciclos voltamétricos
- Generación de matrices PCA con promedios de ciclos 2-5
- Estimación de concentraciones PPM
- Exportación CSV estructurada
- Logging detallado y manejo robusto de errores
===================================================================================
"""

import os
import sys
import logging
import json
import datetime
import traceback
import csv
import joblib
import numpy as np
from pathlib import Path
import logging
log = logging.getLogger(__name__)
from canonical import normalize_classification, display_label_from_label

# ===================================================================================
# BLOQUE 1: CONFIGURACIÓN INICIAL CRÍTICA Y DEPENDENCIAS .NET
# ===================================================================================

def configurar_entorno_python_net():
    """Configuración robusta del entorno Python.NET con validación completa"""
    try:
        # Configurar variable de entorno para Python.NET
        os.environ["PYTHONNET_PYDLL"] = r"C:\\coinvestigacion1\\.venv\\Scripts\\python.exe"
        
        # Método 1: Importación directa (pstrace_session original)
        import pythonnet
        pythonnet.load("coreclr")
        
        # Método 2: Importación CLR adicional (insert_data)
        import clr
        
        from System.Reflection import Assembly
        from System import String, Boolean
        
        logging.info("✓ Entorno .NET inicializado correctamente - Modo híbrido")
        return True, Assembly, String, Boolean, clr
        
    except Exception as e:
        logging.critical("✗ Fallo crítico en dependencias .NET: %s", str(e))
        return False, None, None, None, None

# Inicialización temprana del entorno .NET
net_ok, Assembly, String, Boolean, clr = configurar_entorno_python_net()
if not net_ok:
    sys.exit(1)

# ===================================================================================
# BLOQUE 2: CONFIGURACIÓN AVANZADA DE LOGGING
# ===================================================================================

def configurar_logging_avanzado():
    """Sistema de logging robusto con múltiples salidas y formato mejorado"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s',
        handlers=[
            logging.FileHandler("pstrace_debug.log", encoding='utf-8'),
            logging.StreamHandler(sys.stderr)  # stderr para mantener stdout limpio
        ]
    )
    
    logger = logging.getLogger('PalmSensProcessor')
    logger.info("=" * 60)
    logger.info("INICIANDO PSTRACE SESSION PROCESSOR - VERSIÓN CONSOLIDADA")
    logger.info("=" * 60)
    return logger

log = configurar_logging_avanzado()

# ===================================================================================
# BLOQUE 3: GESTIÓN DE LÍMITES PPM Y CONFIGURACIÓN
# ===================================================================================

def cargar_limites_ppm(ppm_file='limits_ppm.json'):
    """
    Carga los límites de concentración PPM desde archivo JSON

    Args:
        ppm_file (str): Ruta al archivo de límites PPM

    Returns:
        dict: Diccionario con factores de conversión PPM
              Ejemplo: {"Cd":0.10, "Zn":3.00, "Cu":1.00, "Cr":0.50, "Ni":0.50}

              Además, para trazabilidad científica se añade una clave interna
              "_limits_version" con metadatos:
                {
                  "sha256": <hex>|None,
                  "mtime": <float_timestamp>|None,
                  "path": <abs_path>,
                  "load_error": <str>|None
                }

              Notas:
                - No se "inventan" valores: cuando falte alguna clave o el valor
                  sea inválido, el metal tendrá valor None y se registrará la
                  razón en los logs. Otras capas del pipeline deben interpretar
                  None como "límite desconocido" y actuar según la política.
    """
    import hashlib
    from pathlib import Path

    # Claves oficiales esperadas
    claves_oficiales = ["Cd", "Zn", "Cu", "Cr", "Ni"]
    limites_por_defecto = {k: None for k in claves_oficiales}

    ppm_path = Path(ppm_file)

    # Metadatos de versión iniciales
    limits_meta = {"sha256": None, "mtime": None, "path": str(ppm_path.resolve()), "load_error": None}

    try:
        if ppm_path.exists():
            # Leer en bytes para calcular hash y luego decodificar para JSON
            with open(ppm_path, 'rb') as f:
                raw = f.read()

            # SHA256 del archivo (trazabilidad)
            try:
                sha256 = hashlib.sha256(raw).hexdigest()
                limits_meta["sha256"] = sha256
            except Exception as e:
                limits_meta["sha256"] = None
                log.warning("⚠ No se pudo calcular sha256 de %s: %s", ppm_file, str(e))

            # mtime
            try:
                limits_meta["mtime"] = ppm_path.stat().st_mtime
            except Exception:
                limits_meta["mtime"] = None

            # Decodificar y parsear JSON con defensiva
            try:
                text = raw.decode('utf-8')
                parsed = json.loads(text)
                if not isinstance(parsed, dict):
                    raise ValueError("JSON no contiene un objeto/dict en raíz")
            except Exception as e:
                limits_meta["load_error"] = f"json_decode_error: {str(e)}"
                log.error("✗ Error decodificando JSON de límites PPM (%s): %s", ppm_file, str(e))
                # Devolver defaults con metadatos indicando error
                resultados = dict(limites_por_defecto)
                resultados["_limits_version"] = limits_meta
                return resultados

            # Normalizar: asegurar que todas las claves existan y validar numéricos
            resultados = {}
            for metal in claves_oficiales:
                raw_val = parsed.get(metal, None)
                if raw_val is None:
                    resultados[metal] = None
                    log.warning("⚠ Límite para %s no encontrado en JSON, asignando None", metal)
                else:
                    try:
                        val = float(raw_val)
                        # No aceptamos límites no positivos
                        if val <= 0.0:
                            resultados[metal] = None
                            log.warning("⚠ Límite para %s no válido (<=0): %s", metal, raw_val)
                        else:
                            resultados[metal] = val
                    except Exception:
                        resultados[metal] = None
                        log.warning("⚠ Límite para %s no numérico: %s", metal, raw_val)

            # Añadir metadatos de versión
            resultados["_limits_version"] = limits_meta

            log.info("✓ Límites PPM cargados desde %s (version=%s)", ppm_file, limits_meta.get("sha256"))
            return resultados

        else:
            # Archivo no existe: devolver defaults y marcar metadatos
            limits_meta["load_error"] = "file_not_found"
            resultados = dict(limites_por_defecto)
            resultados["_limits_version"] = limits_meta
            log.warning("⚠ Archivo %s no encontrado, usando configuración por defecto", ppm_file)
            return resultados

    except Exception as e:
        # Fallback por error inesperado: devolver defaults con nota de error
        limits_meta["load_error"] = f"unexpected_error: {str(e)}"
        resultados = dict(limites_por_defecto)
        resultados["_limits_version"] = limits_meta
        log.error("✗ Error cargando límites PPM: %s", traceback.format_exc())
        return resultados

# ===================================================================================
# BLOQUE 4: CONFIGURACIÓN Y CARGA DEL SDK PALMSENS
# ===================================================================================

def configurar_sdk_palmsens():
    """
    Configuración robusta del SDK PalmSens con validación de rutas y DLLs
    
    Returns:
        str: Ruta a la DLL principal de PalmSens
    """
    # Construir ruta del SDK
    sdk_path = os.path.abspath(os.path.join(
        os.path.dirname(__file__), '..', 'sdk', 'PSPythonSDK', 'pspython'
    ))
    
    # Validar existencia del SDK
    if not os.path.exists(sdk_path):
        log.critical("✗ Ruta SDK PalmSens inválida: %s", sdk_path)
        sys.exit(1)
    
    # Configurar ruta de la DLL
    dll_path = os.path.join(sdk_path, 'PalmSens.Core.Windows.dll')
    if not os.path.exists(dll_path):
        log.critical("✗ DLL PalmSens no encontrada: %s", dll_path)
        sys.exit(1)
    
    # Agregar SDK al path de Python
    sys.path.insert(0, sdk_path)
    
    try:
        # Importar módulos PalmSens
        import pspymethods
        log.info("✓ SDK PalmSens cargado exitosamente desde: %s", sdk_path)
        log.info("✓ DLL encontrada: %s", dll_path)
        return dll_path
        
    except ImportError as e:
        log.critical("✗ Error importando pspymethods: %s", str(e))
        sys.exit(1)

# ===================================================================================
# BLOQUE 5: CONFIGURACIÓN AVANZADA DEL MÉTODO LOADSESSIONFILE
# ===================================================================================

def cargar_y_configurar_metodo_load(dll_path):
    """
    Carga la DLL y configura dinámicamente el método LoadSessionFile
    Combina las mejores prácticas de ambos códigos originales
    
    Args:
        dll_path (str): Ruta a la DLL de PalmSens
        
    Returns:
        object: Método LoadSessionFile configurado
    """
    try:
        # Cargar ensamblado .NET
        assembly = Assembly.LoadFile(dll_path)
        log.info("✓ Ensamblado .NET cargado: %s", dll_path)
        
        # Obtener tipo de la clase Helper
        tipo = assembly.GetType('PalmSens.Windows.LoadSaveHelperFunctions')
        if not tipo:
            log.critical("✗ Clase LoadSaveHelperFunctions no encontrada")
            sys.exit(1)
        
        # Método 1: Búsqueda por parámetros (pstrace_session original)
        for metodo in tipo.GetMethods():
            if metodo.Name == 'LoadSessionFile':
                params = [p.ParameterType.Name for p in metodo.GetParameters()]
                if params in [['String'], ['String', 'Boolean']]:
                    log.info("✓ LoadSessionFile encontrado - Método 1 - Parámetros: %s", params)
                    return metodo
        
        # Método 2: Búsqueda por tipos CLR (insert_data)
        parametros_posibles = [
            [clr.GetClrType(str)],
            [clr.GetClrType(str), clr.GetClrType(bool)]
        ]
        
        for params in parametros_posibles:
            metodo = tipo.GetMethod("LoadSessionFile", params)
            if metodo:
                log.info("✓ LoadSessionFile encontrado - Método 2 - Tipos CLR: %s", params)
                return metodo
        
        raise AttributeError('LoadSessionFile no encontrado con ningún método')
        
    except Exception as e:
        log.critical("✗ Error crítico cargando método .NET: %s", traceback.format_exc())
        sys.exit(1)

# ===================================================================================
# BLOQUE 6: PROCESAMIENTO AVANZADO DE CICLOS VOLTAMÉTRICOS
# ===================================================================================

def procesar_ciclos_voltametricos(curves):
    """
    Procesamiento avanzado de ciclos voltamétricos según especificaciones.
    Nueva metodología: se elimina el ciclo 1 y se toma únicamente el tercer ciclo
    para análisis (ya no se promedian ciclos 2–5).

    Args:
        curves: Array de curvas voltamétricas

    Returns:
        list: Datos del tercer ciclo (corrientes en float) o lista vacía si falla
    """
    try:
        # Importaciones locales por seguridad (no dependen del scope global)
        import traceback

        # Convertir a lista para manejo uniforme
        arr_curves = list(curves)
        total_ciclos = len(arr_curves)

        log.info("📊 Procesando %d ciclos voltamétricos", total_ciclos)

        # Validar cantidad mínima de ciclos
        if total_ciclos < 3:
            log.warning("⚠ Cantidad insuficiente de ciclos: %d (mínimo: 3)", total_ciclos)
            return []

        # Seleccionar únicamente el tercer ciclo (índice 2)
        tercer_ciclo = arr_curves[2]
        log.info("✓ Ciclo seleccionado para análisis: 3")

        # Extraer valores Y (corrientes) del tercer ciclo
        try:
            corrientes = [float(y) for y in tercer_ciclo.GetYValues()]
            log.debug("  Ciclo 3: %d puntos de corriente extraídos", len(corrientes))
        except Exception as e:
            log.error("✗ Error extrayendo datos del ciclo 3: %s", str(e))
            return []

        # Retornar directamente los valores del tercer ciclo
        log.info("✓ Procesamiento completado: %d puntos obtenidos del ciclo 3", len(corrientes))
        return corrientes

    except Exception as e:
        # Manejo de errores global con traceback
        try:
            import traceback as _tb
            log.error("✗ Error en procesamiento de ciclos: %s", _tb.format_exc())
        except Exception:
            log.error("✗ Error en procesamiento de ciclos: %s", str(e))
        return []
    


# ===================================================================================
# BLOQUE 7: ESTIMACIÓN DE CONCENTRACIONES PPM (VERSIÓN NORMA JSON/0639)
# ===================================================================================

def calcular_estimaciones_ppm(datos_pca, limites_ppm):
    """
    Calcula estimaciones de concentración PPM basadas en los límites oficiales
    definidos en el archivo JSON.

    - Usa los valores PCA como indicadores de contaminación
    - Compara contra cada metal definido en limites_ppm
    - Devuelve un diccionario con, por cada metal, un sub-dict:
         { "ppm": float|null, "pct_of_limit": float|null, "note": str|null }
      donde:
        - "ppm" es la estimación en ppm (si es posible obtenerla; por defecto None)
        - "pct_of_limit" es el porcentaje respecto al límite legal (valor usado para
           decisiones/reglas: 120% → CONTAMINADA, 100% → ANÓMALA, 80% → EN ATENCIÓN)
        - "note" contiene información de validación (por ejemplo "missing_limit")
    - Además devuelve claves auxiliares: "clasificacion" (texto), "max_pct" (float),
      y "method" (metodología usada para la estimación).

    Args:
        datos_pca (list): Datos PCA procesados (valores numéricos representativos)
        limites_ppm (dict): Límites legales de metales (ej. {"Cd":0.1,"Zn":3.0,...})

    Returns:
        dict: Estructura:
          {
            "Cd": {"ppm": None, "pct_of_limit": 12.3, "note": None},
            "Zn": {"ppm": None, "pct_of_limit": None, "note": "missing_limit"},
            ...
            "clasificacion": "SEGURA",
            "max_pct": 12.3,
            "method": "pca_peak_vs_limit"
          }
    """
    if not datos_pca:
        log.warning("⚠ No hay datos PCA para calcular PPM")
        return {}

    try:
        # 1. Obtener un valor representativo desde datos_pca (aquí: valor pico)
        try:
            valor_pico = float(max(datos_pca))
        except Exception as e:
            log.error("✗ No se pudo extraer valor_pico de 'datos_pca': %s", e)
            return {}

        log.info("🔎 Valor pico PCA usado para estimación: %.6f", valor_pico)

        # 2. Preparar resultados por metal con formato claro y trazable
        resultados = {}
        clasificacion = "SEGURA"  # valor inicial
        max_superacion_pct = 0.0  # para determinar la clasificación global

        # Lista ordenada de metales (consistente en todo el pipeline)
        metales = ["Cd", "Zn", "Cu", "Cr", "Ni"]

        for metal in metales:
            # Inicializar sub-dict por metal
            resultados[metal] = {"ppm": None, "pct_of_limit": None, "note": None}

            # Intentar extraer límite numérico para el metal
            limite = None
            try:
                if isinstance(limites_ppm, dict):
                    limite = limites_ppm.get(metal)
            except Exception:
                limite = None

            # Validaciones del límite
            if limite is None:
                resultados[metal]["note"] = "missing_limit"
                log.warning("⚠ Límite para %s ausente en limites_ppm", metal)
                continue

            try:
                limite_val = float(limite)
            except Exception:
                resultados[metal]["note"] = "invalid_limit"
                log.warning("⚠ Límite para %s no numérico: %s", metal, limite)
                continue

            if limite_val <= 0.0:
                resultados[metal]["note"] = "invalid_limit_nonpositive"
                log.warning("⚠ Límite para %s no válido (<=0): %s", metal, limite_val)
                continue

            # Calcular porcentaje respecto al límite: (valor_pico / limite) * 100
            try:
                pct = (valor_pico / limite_val) * 100.0
                # Validar numérico
                pct = float(pct)
                if pct != pct or pct in (float("inf"), float("-inf")):
                    raise ValueError("porcentaje inválido")
            except Exception:
                resultados[metal]["note"] = "calc_error"
                log.warning("⚠ Resultado no numérico para %s (valor_pico=%s, limite=%s)", metal, valor_pico, limite_val)
                continue

            # Guardar pct_of_limit y dejar ppm como None (salvo que exista calibración externa)
            resultados[metal]["pct_of_limit"] = pct
            resultados[metal]["ppm"] = None  # No estimamos ppm directo aquí
            resultados[metal]["note"] = None

            log.debug("  %s: %.2f %% del límite (límite=%.6f)", metal, pct, limite_val)

            # Actualizar máximo porcentaje observado
            if pct > max_superacion_pct:
                max_superacion_pct = pct

        # 3. Determinar clasificación global en función del máximo porcentaje (pct_of_limit)
        if max_superacion_pct >= 120.0:
            clasificacion = "CONTAMINADA"
        elif max_superacion_pct >= 100.0:
            clasificacion = "ANÓMALA"
        elif max_superacion_pct >= 80.0:
            clasificacion = "EN ATENCIÓN"
        else:
            clasificacion = "SEGURA"

        # Añadir metadatos auxiliares para trazabilidad
        resultados["clasificacion"] = clasificacion
        resultados["max_pct"] = float(max_superacion_pct)
        resultados["method"] = "pca_peak_vs_limit"

        log.info("🏷 Clasificación global del agua: %s (%.2f%% máx. superación)", clasificacion, max_superacion_pct)

        return resultados

    except Exception:
        log.error("✗ Error calculando estimaciones PPM: %s", traceback.format_exc())
        return {}

# ===================================================================================
# BLOQUE 7.5: SISTEMA DE CLASIFICACIÓN AVANZADO
# ===================================================================================

class WaterClassifier:
    """
    Sistema avanzado de clasificación de muestras de agua basado en análisis PCA
    y técnicas quimiométricas.
    
    Atributos:
        pca (PCA): Modelo PCA configurado para 2 componentes principales
        threshold (float): Umbral de clasificación para contaminación
        confidence_levels (dict): Niveles de confianza para clasificación
    """
    
    def __init__(self, n_components=2, threshold=0.5):
        """
        Inicializa el clasificador con parámetros configurables.
        
        Args:
            n_components (int): Número de componentes PCA a utilizar
            threshold (float): Umbral para clasificación de contaminación
        """
        try:
            from sklearn.decomposition import PCA
            import numpy as np
            
            self.pca = PCA(n_components=n_components)
            self.threshold = threshold
            self.np = np  # Guardar referencia a numpy
            
            self.confidence_levels = {
                "ALTA": 0.85,
                "MEDIA": 0.65,
                "BAJA": 0.50
            }
            
            log.info("✓ Clasificador inicializado: componentes=%d, umbral=%.2f",
                    n_components, threshold)
            
        except ImportError as e:
            log.error("✗ Error importando dependencias del clasificador: %s", str(e))
            raise
    
    def _preprocess_data(self, voltammetric_data):
        """
        Preprocesa los datos voltamétricos para análisis PCA.
        
        Args:
            voltammetric_data (list/array): Datos crudos de voltametría
            
        Returns:
            array: Datos preprocesados y normalizados
        """
        try:
            # Convertir a array numpy si no lo es
            data = self.np.array(voltammetric_data)
            
            # Remover valores nulos o infinitos
            data = self.np.nan_to_num(data)
            
            # Normalización min-max
            if data.size > 0:
                data_min = self.np.min(data)
                data_max = self.np.max(data)
                if data_max > data_min:
                    data = (data - data_min) / (data_max - data_min)
            
            return data.reshape(1, -1)  # Reshape para PCA
            
        except Exception as e:
            log.error("✗ Error en preprocesamiento: %s", str(e))
            return None
    
    def _calculate_confidence(self, pca_result):
        """
        Calcula el nivel de confianza de la clasificación.
        
        Args:
            pca_result (array): Resultado del análisis PCA
            
        Returns:
            str: Nivel de confianza (ALTA/MEDIA/BAJA)
        """
        try:
            # Calcular distancia al umbral
            max_value = self.np.max(self.np.abs(pca_result))
            distance = self.np.abs(max_value - self.threshold)
            
            # Determinar nivel de confianza
            if distance > self.confidence_levels["ALTA"]:
                return "ALTA"
            elif distance > self.confidence_levels["MEDIA"]:
                return "MEDIA"
            else:
                return "BAJA"
                
        except Exception as e:
            log.error("✗ Error calculando confianza: %s", str(e))
            return "DESCONOCIDA"
    
    def classify_sample(self, voltammetric_data):
        """
        Clasifica una muestra de agua usando técnicas quimiométricas.
        
        Args:
            voltammetric_data (list/array): Datos voltamétricos de la muestra
            
        Returns:
            dict: Resultado de clasificación con formato:
                {
                    "classification": str,  # CONTAMINADA/NO CONTAMINADA
                    "confidence": str,      # ALTA/MEDIA/BAJA
                    "pca_scores": list     # Scores PCA como lista
                }
        """
        try:
            # Validar datos de entrada
            if not voltammetric_data or len(voltammetric_data) == 0:
                log.warning("⚠ Datos voltamétricos vacíos")
                return None
            
            # Preprocesamiento
            processed_data = self._preprocess_data(voltammetric_data)
            if processed_data is None:
                return None
            
            # Análisis PCA
            pca_result = self.pca.fit_transform(processed_data)
            max_value = self.np.max(pca_result)
            
            # Clasificación basada en límites oficiales del JSON (si disponibles)
            classification = "NO CONTAMINADA"
            try:
                # Intentar cargar límites oficiales
                with open("limits_ppm.json", "r") as f:
                    limites_ppm = json.load(f)
                
                # Calcular porcentaje de superación máxima respecto a límites
                max_superacion = 0.0
                for metal in ["Cd", "Zn", "Cu", "Cr", "Ni"]:
                    limite = limites_ppm.get(metal)
                    if limite and limite > 0:
                        porcentaje = (max_value / float(limite)) * 100.0
                        if porcentaje > max_superacion:
                            max_superacion = porcentaje
                
                # Determinar clasificación por porcentaje de superación
                if max_superacion >= 120:
                    classification = "CONTAMINADA"
                elif max_superacion >= 100:
                    classification = "CONTAMINADA"  # anómala pero sobre límite legal
                elif max_superacion >= 80:
                    classification = "NO CONTAMINADA"  # en atención pero bajo límite
                else:
                    classification = "NO CONTAMINADA"
                
                log.info("🏷 Clasificación (JSON): %s (máx. superación: %.2f%%)", classification, max_superacion)
            
            except Exception as e:
                # Fallback a umbral estático si no hay JSON o falla cálculo
                classification = "CONTAMINADA" if max_value > self.threshold else "NO CONTAMINADA"
                log.warning("⚠ Uso de umbral estático por fallo en límites JSON: %s", str(e))
            
            # Calcular confianza
            confidence = self._calculate_confidence(pca_result)
            
            resultado = {
                "classification": classification,
                "confidence": confidence,
                "pca_scores": pca_result.tolist()
            }
            
            log.info("✓ Muestra clasificada: %s (confianza: %s)",
                    classification, confidence)
            
            return resultado
            
        except Exception as e:
            log.error("✗ Error en clasificación: %s", traceback.format_exc())
            return None




# ===================================================================================
# BLOQUE 8: CARGA ROBUSTA DE SESIONES .PSSESSION
# ===================================================================================

def cargar_sesion_pssession(metodo_load, ruta_archivo):
    """
    Carga robusta de archivos .pssession con validación completa
    
    Args:
        metodo_load: Método LoadSessionFile configurado
        ruta_archivo (str): Ruta al archivo .pssession
        
    Returns:
        object or None: Objeto sesión cargado o None si falla
    """
    # Validar existencia del archivo
    if not os.path.exists(ruta_archivo):
        log.error("✗ Archivo .pssession no encontrado: %s", ruta_archivo)
        return None
    
    try:
        # Preparar argumentos según número de parámetros del método
        argumentos = [String(ruta_archivo)]
        num_params = metodo_load.GetParameters().Length
        log.debug("🔧 Método LoadSessionFile detectado con %d parámetros", num_params)

        if num_params == 2:
            argumentos.append(Boolean(False))
        
        # Invocar método de carga
        sesion = metodo_load.Invoke(None, argumentos)
        
        if sesion and hasattr(sesion, "Measurements"):
            mediciones = list(sesion.Measurements)
            log.info("✓ Sesión .pssession cargada exitosamente: %s", ruta_archivo)
            log.info("  Mediciones encontradas: %d", len(mediciones))
            return sesion
        else:
            log.error("✗ La sesión se cargó pero está vacía o no tiene mediciones")
            return None
            
    except FileNotFoundError:
        log.error("✗ Archivo no encontrado al intentar cargar: %s", ruta_archivo)
        return None
    except Exception:
        log.error("✗ Error cargando sesión .pssession: %s", traceback.format_exc())
        return None

# ===================================================================================
# BLOQUE 9: GENERACIÓN AVANZADA DE CSV PCA+PPM
# ===================================================================================

def generar_csv_matriz_pca_ppm(resultados_mediciones):
    """
    Genera archivo CSV con matriz PCA y estimaciones PPM
    Implementa formato estructurado según especificaciones

    NOTA (investigación):
      - Este CSV contiene tanto los porcentajes respecto al límite legal
        (ej. 'Cd_pct' = % del límite) como la predicción global del modelo
        ('ppm_modelo') cuando esté disponible. Además se añaden campos de
        trazabilidad del modelo usados en la predicción por medición.
      - Evitamos ambigüedades renombrando explícitamente las columnas.
    
    Args:
        resultados_mediciones (list): Lista de mediciones procesadas
        
    Returns:
        bool: True si se generó exitosamente, False en caso contrario
    """
    if not resultados_mediciones:
        log.warning("⚠ No hay resultados para generar CSV")
        return False
    
    try:
        # Determinar longitud de datos PCA a partir del primer resultado
        primer_resultado = resultados_mediciones[0]
        longitud_pca = len(primer_resultado.get('pca_scores', []))
        
        if longitud_pca == 0:
            log.warning("⚠ No hay datos PCA para generar CSV")
            return False
        
        # Construir encabezados dinámicos de forma explícita y con unidades claras
        encabezados = ['sensor_id', 'measurement_title']
        encabezados += [f'punto_{i+1}' for i in range(longitud_pca)]  # Puntos PCA / corriente del ciclo 3
        
        # Columnas de porcentaje respecto al límite legal (unidad: %)
        encabezados += ['Cd_pct', 'Zn_pct', 'Cu_pct', 'Cr_pct', 'Ni_pct']
        # Columna con la predicción global del modelo (si existe) en ppm (unidad: ppm)
        encabezados += ['ppm_modelo']
        # Nivel de contaminación (máx % detectado) y clasificación textual
        encabezados += ['contamination_level_pct', 'clasificacion']
        
        # Metadatos del modelo por fila (trazabilidad)
        encabezados += [
            'model_version',
            'model_used_n_features',
            'model_used_baseline',
            'model_baseline_source',
            'model_notes'
        ]
        
        # Crear directorio de salida
        directorio_data = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'data'))
        os.makedirs(directorio_data, exist_ok=True)
        
        # Ruta del archivo CSV
        ruta_csv = os.path.join(directorio_data, 'matriz_pca.csv')
        
        # Escribir CSV con codificación UTF-8
        with open(ruta_csv, 'w', newline='', encoding='utf-8') as archivo_csv:
            escritor = csv.writer(archivo_csv)
            
            # Escribir encabezados
            escritor.writerow(encabezados)
            
            registros_escritos = 0
            # Escribir datos de cada medición
            for resultado in resultados_mediciones:
                fila_datos = [
                    resultado.get('sensor_id', 'N/A'),
                    resultado.get('title', 'Sin título')
                ]
                
                # Agregar datos PCA (ciclo 3 ya procesado en Bloque 10)
                datos_pca = resultado.get('pca_scores', []) or []
                
                # Asegurar que cada fila tenga exactamente 'longitud_pca' valores:
                # - si faltan, se rellenan con None (campo vacío en CSV)
                # - si sobran, se truncan (manteniendo consistencia con encabezados)
                if len(datos_pca) < longitud_pca:
                    padding = [None] * (longitud_pca - len(datos_pca))
                    datos_pca_row = list(datos_pca) + padding
                else:
                    datos_pca_row = list(datos_pca[:longitud_pca])
                
                fila_datos.extend(datos_pca_row)
                
                # Agregar estimaciones PPM (diccionario por metal) - se asume que
                # calcular_estimaciones_ppm devuelve porcentajes (pct) por diseño.
                estimaciones_ppm = resultado.get('ppm_estimations', {}) or {}
                def _safe_get_pct(d, k):
                    v = d.get(k)
                    # Si v es dict y contiene 'pct' o 'pct_of_limit', extraerlo
                    if isinstance(v, dict):
                        return v.get('pct_of_limit') or v.get('pct') or None
                    # Si es numérico, devolverlo
                    try:
                        return float(v) if v is not None else None
                    except Exception:
                        return None
                
                fila_datos.append(_safe_get_pct(estimaciones_ppm, 'Cd'))
                fila_datos.append(_safe_get_pct(estimaciones_ppm, 'Zn'))
                fila_datos.append(_safe_get_pct(estimaciones_ppm, 'Cu'))
                fila_datos.append(_safe_get_pct(estimaciones_ppm, 'Cr'))
                fila_datos.append(_safe_get_pct(estimaciones_ppm, 'Ni'))
                
                # Agregar predicción global del modelo en ppm (si existe)
                ppm_modelo = resultado.get('ppm_modelo', None)
                try:
                    ppm_modelo_val = float(ppm_modelo) if ppm_modelo is not None else None
                except Exception:
                    ppm_modelo_val = None
                fila_datos.append(ppm_modelo_val)
                
                # Agregar nivel de contaminación y clasificación global
                # 'contamination_level' en el resultado ya representa % (según Bloque 10)
                contamination_level = resultado.get('contamination_level', None)
                try:
                    contamination_level_val = float(contamination_level) if contamination_level is not None else None
                except Exception:
                    contamination_level_val = None
                fila_datos.append(contamination_level_val)
                fila_datos.append(resultado.get('clasificacion', 'DESCONOCIDA'))
                
                # Agregar metadatos del modelo (si existen)
                model_meta = resultado.get('model_meta', {}) or {}
                fila_datos.append(model_meta.get('model_version'))
                fila_datos.append(model_meta.get('used_n_features'))
                fila_datos.append(bool(model_meta.get('used_baseline')))
                fila_datos.append(model_meta.get('baseline_source'))
                fila_datos.append(model_meta.get('notes'))
                
                # Normalizar valores None a '' para CSV (mejora legibilidad)
                fila_datos_csv = [("" if v is None else v) for v in fila_datos]
                
                escritor.writerow(fila_datos_csv)
                registros_escritos += 1
        
        log.info("✓ CSV matriz PCA+PPM generado exitosamente: %s", ruta_csv)
        log.info("  Registros escritos: %d", registros_escritos)
        log.info("  Columnas PCA: %d, Columnas PCT por metal: %d, plus ppm_modelo y metadatos", longitud_pca, 5)
        
        return True
        
    except Exception as e:
        log.error("✗ Error generando CSV matriz PCA+PPM: %s", traceback.format_exc())
        return False
    



# ===================================================================================
# BLOQUE 9.5: PREDICCIÓN CON MODELO ENTRENADO (train_memory)
# ===================================================================================

def predecir_con_modelo_entrenado(datos_pca):
    """
    Usa los modelos entrenados (scaler, PCA, RidgeCV) para estimar concentración.
    Se espera que los modelos estén en coinvestigacion1-main/models/.

    Robustez y trazabilidad añadidas:
      - Validaciones de existencia de artefactos.
      - Carga segura de meta.pkl (si existe).
      - Alineado de número de features (padding/truncado) con registro en metadata.
      - Búsqueda de baseline en varios orígenes (meta.pkl, baseline.npy en models/).
      - Política conservadora por defecto: SI NO HAY baseline certificada, NO restar
        la media del propio sample (evita introducir sesgos).
      - Fallbacks documentados para el escalado (scaler.mean_/scale_) y para PCA/predicción.
      - Devolución enriquecida con `model_meta` para auditoría científica.

    Args:
        datos_pca (list or np.ndarray): Corrientes (valores Y del ciclo 3)

    Returns:
        dict: {
            'predicciones': [...],
            'ppm_promedio': float|None,
            'model_meta': {
                'model_version': str|None,
                'used_n_features': int,
                'used_baseline': bool,
                'baseline_source': str|None,
                'notes': str|None
            }
        }
    """
    try:
        import numpy as np
        from pathlib import Path
        import joblib
        import traceback

        ROOT = Path(__file__).resolve().parents[1]
        MODELS_DIR = ROOT / "models"

        scaler_path = MODELS_DIR / "scaler.pkl"
        pca_path = MODELS_DIR / "pca.pkl"
        model_path = MODELS_DIR / "model.pkl"
        meta_path = MODELS_DIR / "meta.pkl"

        # === Validar existencia de modelos ===
        if not (scaler_path.exists() and pca_path.exists() and model_path.exists()):
            log.error("✗ Modelos entrenados no encontrados en 'models/'. Ejecuta train_memory.py primero.")
            return {"predicciones": [], "ppm_promedio": None, "model_meta": {
                "model_version": None, "used_n_features": None, "used_baseline": False, "baseline_source": None,
                "notes": "missing_models"
            }}

        # === Cargar modelos con manejo de errores ===
        try:
            scaler = joblib.load(scaler_path)
            pca = joblib.load(pca_path)
            model = joblib.load(model_path)
        except Exception as e:
            log.error("✗ Error cargando artefactos del modelo: %s", str(e))
            log.debug(traceback.format_exc())
            return {"predicciones": [], "ppm_promedio": None, "model_meta": {
                "model_version": None, "used_n_features": None, "used_baseline": False, "baseline_source": None,
                "notes": "load_error"
            }}

        # Cargar metadata si existe
        meta = {}
        if meta_path.exists():
            try:
                meta = joblib.load(meta_path) or {}
            except Exception as e:
                log.warning("⚠ No se pudo cargar 'meta.pkl' correctamente: %s — continuando sin meta", str(e))
                meta = {}

        # === Preparar entrada X ===
        try:
            X = np.array(datos_pca, dtype=float).reshape(1, -1)
        except Exception as e:
            log.error("✗ Datos de entrada inválidos para predicción: %s", str(e))
            return {"predicciones": [], "ppm_promedio": None, "model_meta": {
                "model_version": meta.get("model_version"), "used_n_features": None, "used_baseline": False,
                "baseline_source": None, "notes": "invalid_input"
            }}
        X = np.nan_to_num(X)

        # === Determinar n_features entrenadas (orden de preferencia) ===
        # 1) meta['n_features'] 2) scaler.mean_.shape[0] 3) X.shape[1] (fallback)
        n_features = None
        if isinstance(meta.get("n_features"), int):
            n_features = int(meta.get("n_features"))
        else:
            scaler_mean = getattr(scaler, "mean_", None)
            if scaler_mean is not None:
                try:
                    n_features = int(np.asarray(scaler_mean).ravel().shape[0])
                except Exception:
                    n_features = None

        if n_features is None:
            n_features = X.shape[1]

        model_meta = {
            "model_version": meta.get("model_version") or getattr(model, "version", None),
            "used_n_features": int(n_features),
            "used_baseline": False,
            "baseline_source": None,
            "notes": None
        }

        # === Alineado de longitudes ===
        if X.shape[1] != n_features:
            try:
                if X.shape[1] < n_features:
                    pad_width = n_features - X.shape[1]
                    X = np.pad(X, ((0, 0), (0, pad_width)), 'constant', constant_values=0.0)
                    model_meta["notes"] = (model_meta.get("notes") or "") + f" padded_{pad_width}"
                else:
                    X = X[:, :n_features]
                    model_meta["notes"] = (model_meta.get("notes") or "") + f" truncated_to_{n_features}"
                log.warning("⚠ Ajustando longitud de entrada a %d características (padding/truncado aplicado)", n_features)
            except Exception as e:
                log.error("✗ Error ajustando longitud de entrada: %s", str(e))
                log.debug(traceback.format_exc())
                return {"predicciones": [], "ppm_promedio": None, "model_meta": model_meta}

        # === Normalización y escalado para PCA (política configurable y trazable) ===
        # Usamos la función centralizada `normalize_for_pca` en src/preprocess.py. Esta función
        # aplica (opcional) resta de baseline y luego un método de escalado configurable.
        try:
            # === Buscar baseline certificada en meta o en models/baseline.npy ===
            baseline_vector = None
            baseline_source = None
            for key in ("baseline", "blank_vector", "baseline_vector", "baseline_mean_vector"):
                if key in meta and meta.get(key) is not None:
                    try:
                        bv = np.array(meta.get(key), dtype=float).reshape(1, -1)
                        if bv.shape[1] != n_features:
                            if bv.shape[1] < n_features:
                                pad_w = n_features - bv.shape[1]
                                bv = np.pad(bv, ((0, 0), (0, pad_w)), 'constant', constant_values=0.0)
                            else:
                                bv = bv[:, :n_features]
                        baseline_vector = bv
                        baseline_source = f"meta:{key}"
                        break
                    except Exception:
                        baseline_vector = None
                        baseline_source = None

            if baseline_vector is None:
                candidate = MODELS_DIR / "baseline.npy"
                if candidate.exists():
                    try:
                        bv = np.load(candidate)
                        bv = np.array(bv, dtype=float).reshape(1, -1)
                        if bv.shape[1] != n_features:
                            if bv.shape[1] < n_features:
                                pad_w = n_features - bv.shape[1]
                                bv = np.pad(bv, ((0, 0), (0, pad_w)), 'constant', constant_values=0.0)
                            else:
                                bv = bv[:, :n_features]
                        baseline_vector = bv
                        baseline_source = "models/baseline.npy"
                    except Exception:
                        baseline_vector = None
                        baseline_source = None

            try:
                from preprocess import normalize_for_pca
            except Exception:
                # Intento alternativo si se ejecuta como paquete
                from .preprocess import normalize_for_pca

            # Método de normalización: se puede definir en meta['normalization_method']
            method = meta.get('normalization_method') or 'use_trained_scaler'

            # Intentar normalizar usando el scaler entrenado (política por defecto)
            try:
                X_scaled, norm_meta = normalize_for_pca(X, baseline_vector=baseline_vector, scaler=scaler, method=method)
                # Actualizar metadatos de modelo para trazabilidad
                model_meta['used_baseline'] = bool(norm_meta.get('used_baseline'))
                model_meta['baseline_source'] = baseline_source
                model_meta['notes'] = (model_meta.get('notes') or '') + f" norm_method:{method}"
            except ValueError as ve:
                # Ocurre si method='use_trained_scaler' pero no se proporcionó scaler.
                log.warning("⚠ normalize_for_pca rechazó el método por falta de artefactos: %s", str(ve))
                # Forzamos un método alternativo seguro: zscore por columnas (no inventa artefactos)
                X_scaled, norm_meta = normalize_for_pca(X, baseline_vector=baseline_vector, scaler=None, method='zscore_columns')
                model_meta['used_baseline'] = bool(norm_meta.get('used_baseline'))
                model_meta['baseline_source'] = baseline_source
                model_meta['notes'] = (model_meta.get('notes') or '') + " fallback:zscore_columns"

        except Exception as e:
            log.error("✗ Error en la normalización para PCA: %s", traceback.format_exc())
            return {"predicciones": [], "ppm_promedio": None, "model_meta": model_meta}

        # === Transformación PCA y predicción con modelo ===
        try:
            X_pca = pca.transform(X_scaled)
        except Exception as e:
            log.error("✗ Error en pca.transform: %s", str(e))
            log.debug(traceback.format_exc())
            return {"predicciones": [], "ppm_promedio": None, "model_meta": model_meta}

        try:
            pred = model.predict(X_pca)
        except Exception as e:
            log.error("✗ Error en model.predict: %s", str(e))
            log.debug(traceback.format_exc())
            return {"predicciones": [], "ppm_promedio": None, "model_meta": model_meta}

        # === Validación de salida y consolidación ===
        try:
            pred_arr = np.asarray(pred).flatten()
            ppm_pred = float(np.mean(pred_arr)) if pred_arr.size > 0 else None
            log.info("💧 Predicción completada → ppm promedio: %s", f"{ppm_pred:.4f}" if ppm_pred is not None else "None")
        except Exception as e:
            log.error("✗ Error consolidando predicciones: %s", str(e))
            log.debug(traceback.format_exc())
            return {"predicciones": [], "ppm_promimo": None, "model_meta": model_meta}

        # Enriquecer notas finales
        if not model_meta.get("notes"):
            model_meta["notes"] = "ok"
        else:
            model_meta["notes"] = model_meta["notes"].strip()

        return {
            "predicciones": pred_arr.tolist(),
            "ppm_promedio": ppm_pred,
            "model_meta": model_meta
        }

    except Exception:
        log.error("✗ Error en predicción con modelo entrenado: %s", traceback.format_exc())
        return {"predicciones": [], "ppm_promedio": None, "model_meta": {
            "model_version": None, "used_n_features": None, "used_baseline": False, "baseline_source": None,
            "notes": "unexpected_error"
        }}


# ===================================================================================
# BLOQUE 10: PROCESADOR PRINCIPAL DE SESIONES
# ===================================================================================

def extraer_y_procesar_sesion_completa(ruta_archivo, limites_ppm):
    """
    Función principal que orquesta todo el procesamiento de sesiones .pssession.
    Nueva metodología: se elimina cualquier lógica de promediar ciclos y se toma
    únicamente el tercer ciclo para el análisis de contaminación, comparando
    directamente contra los límites regulatorios oficiales cargados desde JSON.

    Args:
        ruta_archivo (str): Ruta al archivo .pssession
        limites_ppm (dict): Límites de conversión PPM

    Returns:
        dict or None: Diccionario completo con session_info y measurements
    """
    log.info("🚀 Iniciando procesamiento completo de sesión: %s", ruta_archivo)

    # Paso 1: Configurar SDK y método de carga
    dll_palmsens = configurar_sdk_palmsens()
    metodo_load = cargar_y_configurar_metodo_load(dll_palmsens)

    # Paso 2: Cargar sesión .pssession
    sesion_cargada = cargar_sesion_pssession(metodo_load, ruta_archivo)
    if not sesion_cargada:
        return None

    # Paso 3: Extraer información general de la sesión
    informacion_sesion = {
        'session_id': None,
        'filename': os.path.basename(ruta_archivo),
        'scan_rate': getattr(sesion_cargada, 'ScanRate', None),
        'start_potential': getattr(sesion_cargada, 'StartPotential', None),
        'end_potential': getattr(sesion_cargada, 'EndPotential', None),
        'total_cycles': len(list(sesion_cargada.Measurements)),
        'software_version': getattr(sesion_cargada, 'Version', None),
        'processed_at': datetime.datetime.now().isoformat()
    }

    log.info("📋 Información de sesión extraída: %d mediciones", informacion_sesion['total_cycles'])

    # Paso 4: Procesar cada medición
    resultados_mediciones = []

    for idx, medicion in enumerate(sesion_cargada.Measurements, 1):
        titulo = getattr(medicion, "Title", f"Medición_{idx}")
        log.info("🔬 Procesando medición %d/%d: %s", idx, informacion_sesion['total_cycles'], titulo)

        try:
            # Extraer información básica de la medición
            try:
                timestamp = datetime.datetime(
                    medicion.TimeStamp.Year, medicion.TimeStamp.Month, medicion.TimeStamp.Day,
                    medicion.TimeStamp.Hour, medicion.TimeStamp.Minute, medicion.TimeStamp.Second
                )
            except Exception:
                timestamp = None
                log.warning("⚠ Timestamp no disponible para medición %d", idx)

            info_medicion = {
                'measurement_index': idx,
                'sensor_id': getattr(medicion, 'SensorId', getattr(medicion, 'SensorID', None)),
                'title': titulo,
                'timestamp': timestamp,
                'device_serial': getattr(medicion, 'DeviceUsedSerial', 'N/A'),
                'curve_count': getattr(medicion, 'nCurves', 0)
            }

            # Obtener array de curvas
            array_curvas = medicion.GetCurveArray()
            if not array_curvas:
                log.warning("⚠ Medición %d no contiene curvas", idx)
                continue

            # Procesar curvas individuales (todas, para visualización)
            curvas_detalladas = []
            for idx_curva, curva in enumerate(array_curvas):
                curva_info = {
                    'index': idx_curva,
                    'potentials': [float(x) for x in curva.GetXValues()],
                    'currents': [float(y) for y in curva.GetYValues()]
                }
                curvas_detalladas.append(curva_info)

            # Procesamiento PCA: ahora solo tercer ciclo
            datos_pca = procesar_ciclos_voltametricos(array_curvas)
            # Predicción con el modelo entrenado
            resultado_modelo = predecir_con_modelo_entrenado(datos_pca)
            ppm_predicho = resultado_modelo.get("ppm_promedio")

            if not datos_pca:
                log.warning("⚠ No se pudo procesar PCA para medición %d", idx)
                continue

            # Calcular estimaciones PPM contra límites oficiales
            estimaciones_ppm = calcular_estimaciones_ppm(datos_pca, limites_ppm)

            # Determinar nivel de contaminación correctamente usando los porcentajes ya calculados
            nivel_contaminacion = 0.0

            # Recorremos metales en orden conocido; soportamos dos formatos:
            #  - estimaciones_ppm[metal] == porcentaje (float)  OR
            #  - estimaciones_ppm[metal] == {"pct_of_limit": porcentaje, ...}
            for metal in ["Cd", "Zn", "Cu", "Cr", "Ni"]:
                pct_val = None
                try:
                    if isinstance(estimaciones_ppm, dict):
                        v = estimaciones_ppm.get(metal)
                        if isinstance(v, dict):
                            # compatibilidad futura: extraer pct_of_limit si está presente
                            pct_val = v.get("pct_of_limit") if "pct_of_limit" in v else v.get("pct")
                        else:
                            # formato histórico: directamente porcentaje numérico
                            pct_val = v
                    else:
                        pct_val = None
                except Exception:
                    pct_val = None

                # Normalizar y validar numérico
                try:
                    if pct_val is not None:
                        pct_val = float(pct_val)
                        # ignorar NaN/Inf
                        if pct_val != pct_val or pct_val in (float("inf"), float("-inf")):
                            raise ValueError("valor no numérico")
                        if pct_val > nivel_contaminacion:
                            nivel_contaminacion = pct_val
                except Exception:
                    # ignorar valores inválidos
                    continue

            # Determinar clasificación textual (canónica) usando el máximo % observado
            if nivel_contaminacion >= 120.0:
                raw_label = "CONTAMINADA"
            elif nivel_contaminacion >= 100.0:
                raw_label = "ANOMALA"
            elif nivel_contaminacion >= 80.0:
                raw_label = "ANOMALA"
            else:
                raw_label = "SEGURA"

            # Normalizar a etiqueta canónica y etiqueta de presentación
            try:
                clasificacion = normalize_classification(raw_label)
                display_label = display_label_from_label(clasificacion)
            except Exception:
                # Fallback conservador
                clasificacion = raw_label
                display_label = raw_label

            # Consolidar información completa de la medición
            info_medicion.update({
                'curves': curvas_detalladas,
                'pca_scores': datos_pca,
                'ppm_estimations': estimaciones_ppm,
                'clasificacion': clasificacion,
                'display_label': display_label,
                'contamination_level': nivel_contaminacion,
                'model_meta': resultado_modelo.get('model_meta', {}),
                'ppm_modelo': ppm_predicho,
                'pca_points_count': len(datos_pca) if datos_pca else 0
            })

            resultados_mediciones.append(info_medicion)
            log.info("  ✓ Medición procesada: %d curvas, %d puntos PCA, Clasificación=%s, Nivel=%.2f%%",
                     len(curvas_detalladas), len(datos_pca) if datos_pca else 0,
                     clasificacion, nivel_contaminacion)

        except Exception as e:
            log.error("  ✗ Error procesando medición %d: %s", idx, str(e))
            continue

    # Paso 5: Generar archivo CSV matriz PCA+PPM
    csv_generado = False
    if resultados_mediciones:
        csv_generado = generar_csv_matriz_pca_ppm(resultados_mediciones)
        if csv_generado:
            log.info("✓ Archivo CSV matriz PCA+PPM generado exitosamente")
        else:
            log.warning("⚠ No se pudo generar el archivo CSV")

    # Paso 6: Consolidar resultado final
    resultado_final = {
        'session_info': informacion_sesion,
        'measurements': resultados_mediciones,
        'processing_summary': {
            'total_measurements': len(resultados_mediciones),
            'successful_pca': sum(1 for m in resultados_mediciones if m.get('pca_scores')),
            'csv_generated': csv_generado
        }
    }

    log.info("🎯 Procesamiento completado exitosamente")
    log.info("  📊 Mediciones totales: %d", len(resultados_mediciones))
    log.info("  🧮 PCA exitosos: %d", resultado_final['processing_summary']['successful_pca'])
    log.info("=" * 60)

    return resultado_final

# ===================================================================================
# BLOQUE 11: FUNCIÓN DE INTERFAZ PARA LA GUI - extract_session_dict
# ===================================================================================

def extract_session_dict(filepath):
    """
    Función de interfaz para la GUI que extrae los datos de un archivo .pssession
    en el formato esperado por el sistema de carga.

    Args:
        filepath (str): Ruta al archivo .pssession

    Returns:
        dict: Diccionario con estructura {
            'session_info': dict,
            'measurements': list[dict]
        }
    """
    log.info("🔍 Invocando extract_session_dict para la GUI")
    try:
        # 1. Cargar límites PPM
        limites_ppm = cargar_limites_ppm()

        # 2. Procesar el archivo completo
        resultado_completo = extraer_y_procesar_sesion_completa(filepath, limites_ppm)

        if not resultado_completo:
            log.error("✗ No se pudo procesar el archivo: %s", filepath)
            return None

        # 3. Extraer solo la información requerida por la GUI
        session_info = resultado_completo.get('session_info', {})
        measurements = []

        for m in resultado_completo.get('measurements', []):
            # Detectar scores de PCA bajo cualquiera de las dos claves
            pca_scores = m.get('pca_scores') or m.get('pca_data') or []

            # Asegurar ppm_estimations como dict con todas las claves
            ppm_estimations = m.get('ppm_estimations') or {}
            ppm_estimations = {
                'Cd': ppm_estimations.get('Cd'),
                'Zn': ppm_estimations.get('Zn'),
                'Cu': ppm_estimations.get('Cu'),
                'Cr': ppm_estimations.get('Cr'),
                'Ni': ppm_estimations.get('Ni')
            }

            # Incluir clasificación y nivel de contaminación
            clasificacion = m.get('clasificacion', 'DESCONOCIDA')
            contamination_level = m.get('contamination_level', None)

            measurements.append({
                'title': m.get('title', 'Sin título'),
                'timestamp': m['timestamp'].isoformat()
                             if isinstance(m.get('timestamp'), datetime.datetime)
                             else m.get('timestamp'),
                'device_serial': m.get('device_serial', 'N/A'),
                'curve_count': m.get('curve_count', 0),
                'pca_scores': pca_scores,
                'ppm_estimations': ppm_estimations,
                'clasificacion': clasificacion,
                'contamination_level': contamination_level
                ,
                'model_meta': m.get('model_meta', {})
            })

        # 4. Retornar estructura simplificada
        return {
            'session_info': {
                'filename':          session_info.get('filename'),
                'loaded_at':         session_info.get('processed_at'),
                'scan_rate':         session_info.get('scan_rate'),
                'start_potential':   session_info.get('start_potential'),
                'end_potential':     session_info.get('end_potential'),
                'software_version':  session_info.get('software_version')
            },
            'measurements': measurements
        }

    except Exception:
        log.error("💥 Error crítico en extract_session_dict: %s", traceback.format_exc())
        return None
    
# ===================================================================================
# BLOQUE 12: INTERFAZ PRINCIPAL Y PUNTO DE ENTRADA
# ===================================================================================

def main():
    """
    Función principal del programa
    Maneja argumentos de línea de comandos y orquesta el procesamiento
    """
    try:
        # Validar argumentos de línea de comandos
        if len(sys.argv) != 2:
            log.error("✗ Uso incorrecto del programa")
            log.info("📖 Uso correcto: python pstrace_session.py <ruta_archivo.pssession>")
            sys.exit(1)
        
        ruta_archivo_sesion = sys.argv[1]
        log.info("🎯 Archivo objetivo: %s", ruta_archivo_sesion)

        # Validar extensión del archivo
        if not ruta_archivo_sesion.lower().endswith(".pssession"):
            log.warning("⚠ El archivo no tiene extensión .pssession (se intentará procesar de todas formas)")

        # Cargar límites PPM
        try:
            limites_ppm = cargar_limites_ppm()
        except Exception:
            log.critical("💥 No se pudieron cargar los límites PPM desde JSON")
            sys.exit(1)
        
        # Procesar sesión completa
        resultado_procesamiento = extraer_y_procesar_sesion_completa(ruta_archivo_sesion, limites_ppm)
        
        if resultado_procesamiento:
            # Guardar en la base de datos
            try:
                from db_persistence import guardar_sesion_y_mediciones
                session_id = guardar_sesion_y_mediciones(
                    resultado_procesamiento['session_info'],
                    resultado_procesamiento['measurements']
                )
                if session_id:
                    log.info("💾 Sesión guardada en la BD con id=%s", session_id)
                else:
                    log.warning("⚠ No se pudo guardar la sesión en la BD")
            except Exception as e:
                log.error("✗ Error al guardar en la BD: %s", e)

            # Salida JSON limpia por stdout
            print(json.dumps(resultado_procesamiento, indent=2, ensure_ascii=False, default=str))
            log.info("✅ Procesamiento exitoso - JSON enviado a stdout")
            sys.exit(0)
        else:
            log.error("❌ Fallo en el procesamiento - No se generaron resultados")
            sys.exit(1)
            
    except KeyboardInterrupt:
        log.warning("⚠ Procesamiento interrumpido por el usuario")
        sys.exit(2)
    except Exception:
        log.critical("💥 Error crítico inesperado: %s", traceback.format_exc())
        sys.exit(3)

# ===================================================================================
# PUNTO DE ENTRADA DEL PROGRAMA
# ===================================================================================

if __name__ == '__main__':
    main()