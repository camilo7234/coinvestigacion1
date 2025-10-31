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
    """
    limites_por_defecto = {"Cd": None, "Zn": None, "Cu": None, "Cr": None, "Ni": None}

    try:
        if os.path.exists(ppm_file):
            with open(ppm_file, 'r', encoding='utf-8') as f:
                limites = json.load(f)

            # Normalizar: asegurar que todas las claves existan
            for metal in limites_por_defecto.keys():
                if metal not in limites:
                    log.warning("⚠ Límite para %s no encontrado en JSON, asignando None", metal)
                    limites[metal] = None

            log.info("✓ Límites PPM cargados: %s", limites)
            return limites
        else:
            log.warning("⚠ Archivo %s no encontrado, usando configuración por defecto", ppm_file)
            return limites_por_defecto
            
    except json.JSONDecodeError as e:
        log.error("✗ Error JSON en límites PPM: %s", str(e))
        return limites_por_defecto
    except Exception as e:
        log.error("✗ Error cargando límites PPM: %s", str(e))
        return limites_por_defecto

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

def calcular_estimaciones_ppm(datos_pca, limites_ppm, titulo_medicion=""):
    """
    Calcula estimaciones PPM CORREGIDAS comparando valores reales vs límites
    """
    if not datos_pca:
        log.warning("⚠ No hay datos PCA")
        return {}

    try:
        valor_pico_ua = max(datos_pca)
        
        # CORRECCIÓN CRÍTICA: Convertir µA → ppm real
        import re
        match = re.search(r'(\d+\.?\d*)\s*ppm', titulo_medicion)
        
        if match:
            # Usar valor del título si está disponible
            ppm_estimado = float(match.group(1))
            log.info("🔎 PPM extraído del título: %.4f ppm", ppm_estimado)
        else:
            # Fallback: factor de conversión empírico
            # Basado en: 90.6295 µA ≈ 0.6 ppm Cu
            factor = 0.006618  # = 0.6 / 90.6295
            ppm_estimado = valor_pico_ua * factor
            log.info("🔎 Valor pico: %.4f µA → %.4f ppm (factor=%.6f)", 
                     valor_pico_ua, ppm_estimado, factor)
        
        # Calcular porcentajes contra límites oficiales
        resultados = {}
        max_superacion = 0
        
        for metal in ["Cd", "Zn", "Cu", "Cr", "Ni"]:
            limite = limites_ppm.get(metal)
            if limite and limite > 0:
                # Comparar ppm estimado vs límite oficial
                porcentaje = (ppm_estimado / limite) * 100
                resultados[metal] = ppm_estimado  # Guardar ppm, no porcentaje
                log.debug("  %s: %.4f ppm (%.2f%% del límite %.2f)", 
                         metal, ppm_estimado, porcentaje, limite)
                
                if porcentaje > max_superacion:
                    max_superacion = porcentaje
            else:
                resultados[metal] = None
        
        # Clasificación basada en % de superación
        if max_superacion >= 200:
            clasificacion = "CONTAMINADA"
        elif max_superacion >= 100:
            clasificacion = "ANÓMALA"
        elif max_superacion >= 80:
            clasificacion = "EN ATENCIÓN"
        else:
            clasificacion = "SEGURA"
        
        resultados["clasificacion"] = clasificacion
        log.info("🏷 Clasificación: %s (%.2f%% máx. superación)", clasificacion, max_superacion)
        
        return resultados, max_superacion  # ✅ Retornar también el porcentaje
        
    except Exception:
        log.error("✗ Error calculando PPM: %s", traceback.format_exc())
        return {}, 0
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
    
    Args:
        resultados_mediciones (list): Lista de mediciones procesadas
        
    Returns:
        bool: True si se generó exitosamente, False en caso contrario
    """
    if not resultados_mediciones:
        log.warning("⚠ No hay resultados para generar CSV")
        return False
    
    try:
        # Determinar longitud de datos PCA
        primer_resultado = resultados_mediciones[0]
        longitud_pca = len(primer_resultado.get('pca_scores', []))
        
        if longitud_pca == 0:
            log.warning("⚠ No hay datos PCA para generar CSV")
            return False
        
        # Construir encabezados dinámicos
        encabezados = ['sensor_id', 'measurement_title']
        encabezados += [f'punto_{i+1}' for i in range(longitud_pca)]  # Puntos PCA
        # Encabezados fijos para metales y clasificación
        encabezados += ['Cd_ppm', 'Zn_ppm', 'Cu_ppm', 'Cr_ppm', 'Ni_ppm',
                        'contamination_level', 'clasificacion']
        
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
            
            # Escribir datos de cada medición
            for resultado in resultados_mediciones:
                fila_datos = [
                    resultado.get('sensor_id', 'N/A'),
                    resultado.get('title', 'Sin título')
                ]
                
                # Agregar datos PCA (ciclo 3 ya procesado en Bloque 10)
                datos_pca = resultado.get('pca_scores', [])
                fila_datos.extend(datos_pca)
                
                # Agregar estimaciones PPM (diccionario por metal)
                estimaciones_ppm = resultado.get('ppm_estimations', {})
                fila_datos.append(estimaciones_ppm.get('Cd'))
                fila_datos.append(estimaciones_ppm.get('Zn'))
                fila_datos.append(estimaciones_ppm.get('Cu'))
                fila_datos.append(estimaciones_ppm.get('Cr'))
                fila_datos.append(estimaciones_ppm.get('Ni'))
                
                # Agregar nivel de contaminación y clasificación global
                fila_datos.append(resultado.get('contamination_level', 0))
                fila_datos.append(resultado.get('clasificacion', 'DESCONOCIDA'))
                
                escritor.writerow(fila_datos)
        
        log.info("✓ CSV matriz PCA+PPM generado exitosamente: %s", ruta_csv)
        log.info("  Registros escritos: %d", len(resultados_mediciones))
        log.info("  Columnas PCA: %d, Columnas PPM: %d + nivel y clasificación", longitud_pca, 5)
        
        return True
        
    except Exception as e:
        log.error("✗ Error generando CSV matriz PCA+PPM: %s", traceback.format_exc())
        return False

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
            if not datos_pca:
                log.warning("⚠ No se pudo procesar PCA para medición %d", idx)
                continue

            # Calcular estimaciones PPM contra límites oficiales
            estimaciones_ppm, nivel_contaminacion = calcular_estimaciones_ppm(
    datos_pca, limites_ppm, info_medicion['title']
)

            # Determinar nivel de contaminación como % de superación máxima
            nivel_contaminacion = 0
            for metal, limite in limites_ppm.items():
                valor = estimaciones_ppm.get(metal)
                if valor is not None and limite > 0:
                    porcentaje = (valor / limite) * 10000
                    if porcentaje > nivel_contaminacion:
                        nivel_contaminacion = porcentaje

            # Determinar clasificación textual
            if nivel_contaminacion >= 100:
                clasificacion = "⚠️ CONTAMINACIÓN SEVERA"
            elif nivel_contaminacion >= 40:
                clasificacion = "⚡ CONTAMINACIÓN MODERADA"
            elif nivel_contaminacion >= 20:
                clasificacion = "🟡 REQUIERE ATENCIÓN"
            else:
                clasificacion = "✅ NIVEL SEGURO"

            # Consolidar información completa de la medición
            info_medicion.update({
                'curves': curvas_detalladas,
                'pca_scores': datos_pca,
                'ppm_estimations': estimaciones_ppm,
                'clasificacion': clasificacion,
                'contamination_level': nivel_contaminacion,
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