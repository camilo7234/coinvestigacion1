"""
pstrace_connection.py

Módulo para obtener datos del potenciostato en tiempo real.
Si no hay dispositivo, reutiliza la carga desde un archivo .pssession.
"""

import os
import sys
import logging
from device_events import event_manager, DeviceEvent
# ===================================================================================
# BLOQUE 1: CONFIGURACIÓN E INICIALIZACIÓN DEL ENTORNO  
# ===================================================================================

# 1) Añadir ruta raíz del proyecto para importar pstrace_session
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# 2) Importar funciones de pstrace_session
try:
    from src.pstrace_session import (
        cargar_sesion_pssession,
        configurar_entorno_python_net,
        configurar_sdk_palmsens,
        extract_session_dict,
        cargar_limites_ppm,
        extraer_y_procesar_sesion_completa,
        calcular_estimaciones_ppm
    )
    # Alias internos para consistencia en este módulo
    cargar_sesion = cargar_sesion_pssession
    extraer_generar = extract_session_dict
    cargar_limites = cargar_limites_ppm

    # Inyectar alias directamente en el módulo pstrace_session
    import sys
    import src.pstrace_session as _ps
    _ps.cargar_sesion = cargar_sesion_pssession
    _ps.extraer_generar = extract_session_dict
    _ps.cargar_limites = cargar_limites_ppm

    import logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    log = logging.getLogger("pstrace_connection")
    log.info("✓ pstrace_session importado correctamente con alias de compatibilidad.")
except ImportError as e:
    import logging, sys
    logging.error("✗ Error al importar pstrace_session: %s", e)
    sys.exit(1)

# 3) Inicializar entorno .NET y SDK PalmSens
net_ok, Assembly, String, Boolean, clr = configurar_entorno_python_net()
if not net_ok:
    sys.exit(1)

dll_path = configurar_sdk_palmsens()
try:
    clr.AddReference(dll_path)
    log.info("✓ SDK PalmSens inicializado en pstrace_connection")
except Exception as e:
    log.error("✗ Error cargando referencia PalmSens: %s", e)
    sys.exit(1)

# ===================================================================================
# BLOQUE 2: DESCUBRIMIENTO DE DISPOSITIVOS
# ===================================================================================

from typing import List, Dict
import logging
from dataclasses import dataclass
from enum import Enum
import asyncio

class TransportType(Enum):
    USB = "USB"
    BLUETOOTH = "Bluetooth" 
    TCP = "TCP"

@dataclass
class DeviceInfo:
    name: str
    serial: str = None 
    transport: TransportType = None
    address: str = None
    status: str = "unknown"

def descubrir_instrumentos() -> List[Dict]:
    """
    Descubre instrumentos disponibles (USB, Bluetooth, TCP).
    
    Returns:
        List[Dict]: Lista de dispositivos encontrados con formato:
        {
            'name': str,
            'serial': str|None,
            'transport': 'USB'|'Bluetooth'|'TCP',
            'address': 'COMx'|'BT:AA:BB:...'|'host:port',
            'status': str
        }
        
    Raises:
        RuntimeError: Si hay error crítico al cargar SDK
    """
    dispositivos = []
    try:
        import pspymethods
        log = logging.getLogger(__name__)

        # Diccionario de métodos de descubrimiento
        discovery_methods = {
            TransportType.USB: getattr(pspymethods, "list_usb_devices", None),
            TransportType.BLUETOOTH: getattr(pspymethods, "list_bluetooth_devices", None),
            TransportType.TCP: getattr(pspymethods, "list_tcp_endpoints", None)
        }

        # Intentar cada método de descubrimiento
        for transport_type, discovery_method in discovery_methods.items():
            if discovery_method is None:
                log.warning(f"Método de descubrimiento no disponible para {transport_type.value}")
                continue

            try:
                device_list = discovery_method()
                
                # Procesar dispositivos encontrados
                for dev in device_list:
                    device_info = DeviceInfo(
                        name=getattr(dev, 'Name', 'PalmSens'),
                        serial=getattr(dev, 'SerialNumber', None),
                        transport=transport_type,
                        address=_get_device_address(dev, transport_type),
                        status='available'
                    )
                    
                    if _validar_dispositivo(device_info):
                        dispositivos.append(device_info.__dict__)
                    else:
                        log.warning(f"Dispositivo inválido encontrado: {device_info}")

            except Exception as e:
                log.error(f"Error descubriendo dispositivos {transport_type.value}: {str(e)}")
                continue

        # Logging detallado del resultado
        log.info(f"✓ Descubiertos {len(dispositivos)} dispositivos")
        for dev in dispositivos:
            log.debug(f"Dispositivo encontrado: {dev}")
            
        return dispositivos

    except ImportError as e:
        log.critical(f"✗ Error crítico: No se pudo cargar SDK PalmSens: {str(e)}")
        raise RuntimeError("SDK PalmSens no disponible") from e
    except Exception as e:
        log.exception("✗ Error durante la descubierta de instrumentos")
        return []

def _get_device_address(dev, transport_type: TransportType) -> str:
    """Obtiene la dirección formateada según el tipo de transporte"""
    if transport_type == TransportType.USB:
        return getattr(dev, 'PortName', None)
    elif transport_type == TransportType.BLUETOOTH:
        return f"BT:{getattr(dev, 'Address', None)}"
    elif transport_type == TransportType.TCP:
        return getattr(dev, 'Endpoint', None)
    return None

def _validar_dispositivo(device_info: DeviceInfo) -> bool:
    """Valida que el dispositivo tenga los campos mínimos necesarios"""
    return all([
        device_info.name,
        device_info.transport,
        device_info.address is not None
    ])

async def verificar_conectividad_dispositivo(device_info: Dict) -> bool:
    """
    Verifica que el dispositivo esté realmente disponible
    
    Args:
        device_info (Dict): Información del dispositivo a verificar
        
    Returns:
        bool: True si el dispositivo responde, False en caso contrario
    """
    try:
        # Implementar verificación según tipo de transporte
        if device_info['transport'] == TransportType.TCP.value:
            host, port = device_info['address'].split(':')
            reader, writer = await asyncio.open_connection(host, int(port))
            writer.close()
            await writer.wait_closed()
            return True
            
        # Para USB y Bluetooth podemos asumir que si fueron detectados están disponibles
        return True
        
    except Exception as e:
        logging.error(f"Error verificando dispositivo {device_info['name']}: {str(e)}")
        return False
    

# ===================================================================================
# BLOQUE 3: CONEXIÓN Y DESCONEXIÓN DE INSTRUMENTOS
# ===================================================================================

import asyncio
from typing import Optional, Dict
from contextlib import contextmanager

class PalmSensConnectionError(Exception):
    """Error personalizado para fallos de conexión PalmSens"""
    pass

class ConnectionManager:
    """Gestor de conexiones para mantener estado y reconexión"""
    _instance = None
    _active_connections = {}
    
    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            cls._instance = ConnectionManager()
        return cls._instance
    
    def register_connection(self, device_id: str, connection):
        self._active_connections[device_id] = {
            'connection': connection,
            'last_heartbeat': asyncio.get_event_loop().time(),
            'reconnect_attempts': 0
        }
    
    def remove_connection(self, device_id: str):
        self._active_connections.pop(device_id, None)

@contextmanager
def connection_context(device_id: str, connection):
    """Contexto para gestionar conexiones automáticamente"""
    manager = ConnectionManager.get_instance()
    try:
        manager.register_connection(device_id, connection)
        yield connection
    finally:
        manager.remove_connection(device_id)

async def check_device_health(instrumento) -> bool:
    """Verifica el estado de salud del dispositivo"""
    try:
        estado = await asyncio.to_thread(estado_instrumento, instrumento)
        return estado['connected'] and not estado['last_error']
    except Exception:
        return False

def conectar_instrumento(serial: str = None,
                         transport: str = None,
                         address: str = None,
                         timeout_ms: int = 10000,
                         max_retries: int = 3,
                         retry_delay: float = 1.0):
    """
    Conecta con un instrumento PalmSens usando serial o address.
    Retorna el objeto 'instrumento' del SDK.
    
    Args:
        serial (str, optional): Número de serie del dispositivo
        transport (str, optional): Tipo de transporte (USB/Bluetooth/TCP)
        address (str, optional): Dirección del dispositivo
        timeout_ms (int): Timeout en milisegundos
        max_retries (int): Número máximo de intentos de conexión
        retry_delay (float): Tiempo entre reintentos en segundos
    
    Returns:
        Object: Objeto instrumento del SDK
        
    Raises:
        PalmSensConnectionError: Si la conexión falla después de todos los reintentos
    """
    dispositivos = descubrir_instrumentos()
    if not dispositivos:
        raise PalmSensConnectionError("No hay instrumentos disponibles.")

    # Selección del dispositivo
    objetivo = None
    if serial:
        matches = [d for d in dispositivos if d.get('serial') == serial]
        if not matches:
            raise PalmSensConnectionError(f"No se encontró instrumento con serial {serial}.")
        objetivo = matches[0]
    elif address:
        matches = [d for d in dispositivos if d.get('address') == address]
        if not matches:
            raise PalmSensConnectionError(f"No se encontró instrumento con address {address}.")
        objetivo = matches[0]
    else:
        objetivo = dispositivos[0]  # fallback: primer dispositivo

    if transport:
        objetivo['transport'] = transport

    log.info(f"→ Conectando a {objetivo['name']} "
             f"serial={objetivo.get('serial')} "
             f"via {objetivo['transport']} @ {objetivo.get('address')}")

    last_exception = None
    for attempt in range(max_retries):
        try:
            import pspymethods
            instrumento = None

            # Timeouts específicos por tipo de transporte
            transport_timeouts = {
                'USB': timeout_ms,
                'Bluetooth': timeout_ms * 2,  # Bluetooth necesita más tiempo
                'TCP': timeout_ms
            }
            
            current_timeout = transport_timeouts.get(objetivo['transport'], timeout_ms)

            # Ajusta a los métodos reales de tu SDK
            if objetivo['transport'] == 'USB' and hasattr(pspymethods, "connect_usb"):
                instrumento = pspymethods.connect_usb(objetivo['address'], current_timeout)
            elif objetivo['transport'] == 'Bluetooth' and hasattr(pspymethods, "connect_bluetooth"):
                instrumento = pspymethods.connect_bluetooth(objetivo['address'], current_timeout)
            elif objetivo['transport'] == 'TCP' and hasattr(pspymethods, "connect_tcp"):
                host, port = objetivo['address'].split(':')
                instrumento = pspymethods.connect_tcp(host, int(port), current_timeout)
            else:
                raise PalmSensConnectionError(f"Transporte no soportado o método no disponible: {objetivo['transport']}")

            if instrumento is None:
                raise PalmSensConnectionError("El SDK no retornó objeto instrumento (conexión fallida).")

            # Verificar estado del dispositivo
            if asyncio.run(check_device_health(instrumento)):
                log.info(f"✓ Conexión establecida correctamente (intento {attempt + 1}/{max_retries})")
                
                # Registrar conexión en el gestor
                device_id = objetivo.get('serial') or objetivo.get('address')
                # Emitir evento de conexión
                try:
                    event_manager.emit_nowait('device_connected', {'serial': device_id, 'transport': objetivo.get('transport')}, device_id)
                except Exception:
                    log.debug("No se pudo emitir evento device_connected")

                with connection_context(device_id, instrumento):
                    return instrumento
            else:
                raise PalmSensConnectionError("Dispositivo no responde correctamente")

        except Exception as e:
            last_exception = e
            log.warning(f"Intento {attempt + 1}/{max_retries} fallido: {str(e)}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
            continue

    log.exception("✗ Error durante la conexión al instrumento")
    raise PalmSensConnectionError(f"Conexión fallida después de {max_retries} intentos: {str(last_exception)}")

def estado_instrumento(instrumento) -> dict:
    """
    Retorna estado resumido del instrumento.
    """
    est = {
        'connected': True,
        'device_serial': None,
        'firmware': None,
        'battery': None,
        'last_error': None,
    }
    try:
        est['device_serial'] = getattr(instrumento, 'SerialNumber', None)
        est['firmware'] = getattr(instrumento, 'FirmwareVersion', None)
        if hasattr(instrumento, 'BatteryLevel'):
            est['battery'] = instrumento.BatteryLevel
    except Exception as e:
        est['connected'] = False
        est['last_error'] = str(e)
    log.debug(f"Estado instrumento: {est}")
    return est

def desconectar_instrumento(instrumento):
    """
    Cierra la conexión usando el método del SDK correspondiente.
    """
    try:
        if hasattr(instrumento, "Disconnect"):
            instrumento.Disconnect()
        else:
            import pspymethods
            if hasattr(pspymethods, "disconnect"):
                pspymethods.disconnect(instrumento)
        
        # Limpiar del gestor de conexiones
        device_id = getattr(instrumento, 'SerialNumber', None)
        if device_id:
            ConnectionManager.get_instance().remove_connection(device_id)
            try:
                event_manager.emit_nowait('device_disconnected', {'serial': device_id}, device_id)
            except Exception:
                log.debug("No se pudo emitir device_disconnected")
            
        log.info("✓ Instrumento desconectado correctamente.")
    except Exception:
        log.exception("✗ Error al desconectar instrumento")



# ===================================================================================
# BLOQUE 4: MEDICIÓN CV REMOTA Y NORMALIZACIÓN
# ===================================================================================

import datetime
from typing import Dict, List, Optional
import asyncio
from collections import deque

async def iniciar_medicion_cv_remota(instrumento, method_params: dict) -> dict:
    """
    Ejecuta una medición CV con eventos y streaming en tiempo real.
    
    Args:
        instrumento: Objeto instrumento PalmSens
        method_params: Parámetros de configuración CV
        
    Returns:
        dict: Datos normalizados y metadata de la sesión
    """
    # Buffer circular para datos en tiempo real
    buffer_datos = deque(maxlen=10000)
    device_id = getattr(instrumento, 'SerialNumber', "Unknown")

    try:
        import pspymethods

        # 1) Validar y normalizar parámetros
        method_params = _validar_parametros_cv(method_params)
        
        # 2) Configurar método CV con eventos
        if hasattr(pspymethods, "configure_cv"):
            # Emitir evento de inicio de configuración (no bloquear)
            event_manager.emit_nowait('cv_config_start', method_params, device_id)
            
            pspymethods.configure_cv(instrumento, **method_params)
            log.info("✓ Método CV configurado con parámetros: %s", method_params)
            
            event_manager.emit_nowait('cv_config_complete', {"status": "configured"}, device_id)
        else:
            raise PalmSensConnectionError("configure_cv no disponible en SDK")

        # 3) Configurar callback para streaming
        async def data_callback(punto_medicion):
            """Callback para procesar datos en tiempo real"""
            try:
                datos = {
                    "potential": float(punto_medicion.Potential),
                    "current": float(punto_medicion.Current),
                    "timestamp": datetime.datetime.now()
                }
                
                # Almacenar en buffer
                buffer_datos.append(datos)
                
                # Emitir evento de datos
                # Emitir sin bloquear el SDK
                event_manager.emit_nowait('cv_data_point', datos, device_id)
                
                # Registrar actividad del dispositivo
                await event_manager.register_heartbeat(device_id)
                
            except Exception as e:
                event_manager.emit_nowait('cv_data_error', {"error": str(e)}, device_id)

        # 4) Ejecutar medición con streaming
        if not hasattr(pspymethods, "run_cv_streaming"):
            raise PalmSensConnectionError("SDK no soporta streaming")
            
        event_manager.emit_nowait('cv_measurement_start', {"mode": "streaming"}, device_id)
        
        data = await pspymethods.run_cv_streaming(
            instrumento,
            callback=data_callback,
            buffer_size=method_params.get("buffer_size", 1000)
        )

    # 5) Normalizar y procesar curvas
        curvas_normalizadas = _normalizar_curvas(list(buffer_datos))

        # 6) Construir respuesta
        session_info = {
            "scan_rate": method_params.get("scan_rate"),
            "start_potential": method_params.get("start_potential"),
            "end_potential": method_params.get("end_potential"),
            "software_version": "PSTrace 5.9.3803",
            "streaming_enabled": True,
            "buffer_size": len(buffer_datos)
        }

        measurement = {
            "title": "CV Streaming",
            "timestamp": datetime.datetime.now(),
            "device_serial": device_id,
            "curve_count": len(curvas_normalizadas),
            "curves": curvas_normalizadas,
        }

        # 7) Emitir evento de finalización
        event_manager.emit_nowait('cv_measurement_complete', measurement, device_id)

        log.info(f"✓ Medición streaming completada: {measurement['curve_count']} curvas")
        return {
            "session_info": session_info,
            "measurements": [measurement],
        }

    except Exception as e:
        log.exception("✗ Error durante medición CV streaming")
        event_manager.emit_nowait('cv_error', {"error": str(e)}, device_id)
        raise PalmSensConnectionError(f"Error en medición CV: {str(e)}")


def simulate_stream_from_pssession(metodo_load, ruta_archivo, rate_hz: float = 10.0, device_id: str = "SIM_PSTRACE", max_points: int = None):
    """
    Simula un streaming a partir de un archivo .pssession emitiendo eventos 'cv_data_point'.

    Args:
        metodo_load: método LoadSessionFile configurado (de pstrace_session)
        ruta_archivo: ruta al archivo .pssession
        rate_hz: frecuencia de emisión en Hz
        device_id: identificador del dispositivo simulado
    """
    try:
        # Si no se pasó metodo_load intentar construirlo automáticamente
        if metodo_load is None:
            try:
                dll = configurar_sdk_palmsens()
                # _ps fue importado arriba como alias de src.pstrace_session
                metodo_load = getattr(__import__('src.pstrace_session', fromlist=['cargar_y_configurar_metodo_load']), 'cargar_y_configurar_metodo_load')(dll)
            except Exception as e:
                log.warning("No se pudo construir metodo_load automático: %s", e)

        # Procesar la sesión usando las funciones maestras de pstrace_session
        try:
            limites = cargar_limites_ppm()
            resultado = extraer_y_procesar_sesion_completa(ruta_archivo, limites)
        except Exception as e:
            log.exception("Fallo procesando sesión para simulación: %s", e)
            resultado = None

        if not resultado:
            log.error("No se pudo procesar la sesión para simulación: %s", ruta_archivo)
            return

        measurements = resultado.get('measurements', [])
        if not measurements:
            log.error("Sesión procesada no contiene mediciones para simular")
            return

        # Usar la primera medición procesada
        first = measurements[0]
        # Emitir configuración/metadata previa a la simulación
        try:
            event_manager.emit_nowait('cv_config', {
                'title': first.get('title'),
                'device_serial': first.get('device_serial'),
                'curve_count': first.get('curve_count'),
                'session_info': resultado.get('session_info')
            }, device_id)
        except Exception:
            log.debug("No se pudo emitir cv_config")

        # 'curves' en la estructura resultante contiene listas de puntos dicts
        curves = first.get('curves') or []

        # Construir lista de puntos iterables
        points = []
        for curva in curves:
            # curva es {'potentials': [...], 'currents': [...]}
            pots = curva.get('potentials') or []
            curs = curva.get('currents') or []
            for i in range(min(len(pots), len(curs))):
                points.append({
                    'potential': float(pots[i]),
                    'current': float(curs[i]),
                    'timestamp': datetime.datetime.now()
                })

        if not points:
            log.error("No se encontraron puntos en la sesión para simular")
            return

        interval = 1.0 / max(0.1, rate_hz)
        log.info("Iniciando simulación de streaming desde %s a %.2f Hz (%d puntos)", ruta_archivo, rate_hz, len(points))
        import time
        sent = 0
        for pt in points:
            if max_points is not None and sent >= max_points:
                break

            # ajustar timestamp: si measurement tiene timestamp usarlo como base
            try:
                base_ts = first.get('timestamp')
                if base_ts and isinstance(base_ts, datetime.datetime):
                    # incrementar microsegundos según índice y rate
                    delta_seconds = sent * interval
                    pt_ts = base_ts + datetime.timedelta(seconds=delta_seconds)
                    pt['timestamp'] = pt_ts
                else:
                    pt['timestamp'] = datetime.datetime.now()
            except Exception:
                pt['timestamp'] = datetime.datetime.now()

            event_manager.emit_nowait('cv_data_point', pt, device_id)

            # registrar heartbeat en background de forma robusta
            try:
                loop = None
                try:
                    loop = asyncio.get_event_loop()
                    if loop.is_running():
                        asyncio.run_coroutine_threadsafe(event_manager.register_heartbeat(device_id), loop)
                    else:
                        # crear loop temporal para registro
                        asyncio.run(event_manager.register_heartbeat(device_id))
                except RuntimeError:
                    # no hay loop asociado al hilo actual
                    try:
                        asyncio.run(event_manager.register_heartbeat(device_id))
                    except Exception:
                        pass
            except Exception:
                pass

            sent += 1
            time.sleep(interval)

        # Al finalizar, calcular estimaciones PPM para la simulación si es posible
        try:
            # datos_pca: tomar corrientes del tercer ciclo si existe
            datos_pca = None
            if len(curves) >= 3:
                datos_pca = curves[2].get('currents')
            elif len(curves) > 0:
                datos_pca = curves[0].get('currents')

            ppm_result = None
            if datos_pca:
                ppm_result = calcular_estimaciones_ppm(datos_pca, limites)

            event_manager.emit_nowait('cv_measurement_complete', {'simulated': True, 'points': len(points), 'ppm': ppm_result}, device_id)
        except Exception as e:
            log.debug("Error calculando PPM en simulación: %s", e)
            event_manager.emit_nowait('cv_measurement_complete', {'simulated': True, 'points': len(points)}, device_id)

    except Exception as e:
        log.exception("Error en simulate_stream_from_pssession: %s", e)
        event_manager.emit_nowait('cv_error', {'error': str(e)}, device_id)

def _validar_parametros_cv(params: Dict) -> Dict:
    """Valida y normaliza parámetros CV"""
    required = ["scan_rate", "start_potential", "end_potential"]
    for param in required:
        if param not in params:
            raise ValueError(f"Falta parámetro requerido: {param}")
            
    if not (0.001 <= params["scan_rate"] <= 1000):
        raise ValueError("scan_rate debe estar entre 0.001 y 1000 V/s")
        
    return params

def _normalizar_curvas(buffer_datos: List[Dict]) -> List[List[Dict]]:
    """Convierte buffer de datos en curvas normalizadas"""
    curvas = []
    curva_actual = []
    
    for punto in buffer_datos:
        curva_actual.append({
            "potential": punto["potential"],
            "current": punto["current"]
        })
        
        # Detectar fin de ciclo (cambio en dirección del potencial)
        if len(curva_actual) > 1:
            if (curva_actual[-1]["potential"] < curva_actual[-2]["potential"] and
                len(curva_actual) > 100):  # mínimo de puntos
                curvas.append(curva_actual)
                curva_actual = []
                
    if curva_actual:  # agregar última curva
        curvas.append(curva_actual)
        
    return curvas


# ===================================================================================
# BLOQUE 5: ORQUESTACIÓN CON BD Y GUI
# ===================================================================================

from src.db_connection import conectar_bd as get_connection
from src.insert_data import guardar_sesion, guardar_mediciones
# Nota: ajusta los imports según tu estructura real de proyecto

def ejecutar_sesion_remota(serial: str, method_params: dict, gui_refresh_callback=None):
    """
    Orquesta una sesión remota completa:
    - Conecta al instrumento
    - Ejecuta medición CV
    - Inserta datos en BD
    - Refresca GUI si se pasa callback

    Args:
        serial (str): Número de serie del instrumento
        method_params (dict): Parámetros del método CV
        gui_refresh_callback (callable): Función opcional para refrescar GUI
    """
    conn = None
    instrumento = None
    try:
        # 1) Conexión
        instrumento = conectar_instrumento(serial=serial)

        # 2) Medición remota
        datos = iniciar_medicion_cv_remota(instrumento, method_params)

        # 3) Conexión a BD
        conn = get_connection()

        # 4) Guardar sesión y mediciones
        session_id = guardar_sesion(
            conn,
            filename=f"REMOTE_{serial}",
            info=datos['session_info']
        )
        guardar_mediciones(conn, session_id, datos['measurements'])

        log.info(f"✓ Sesión remota guardada en BD con ID {session_id}")

        # 5) Refrescar GUI
        if gui_refresh_callback:
            gui_refresh_callback()
            log.info("✓ GUI refrescada tras inserción de sesión remota")

        return session_id

    except Exception:
        log.exception("✗ Error durante la ejecución de sesión remota")
        raise
    finally:
        if instrumento:
            desconectar_instrumento(instrumento)
        if conn:
            conn.close()

# ===================================================================================
# BLOQUE 6: ROBUSTEZ Y RESILIENCIA
# ===================================================================================

import time
import random

def conectar_con_reintentos(serial: str = None,
                            address: str = None,
                            intentos: int = 3,
                            base_delay: float = 1.0):
    """
    Intenta conectar al instrumento con reintentos y backoff exponencial.
    """
    for intento in range(1, intentos + 1):
        try:
            instr = conectar_instrumento(serial=serial, address=address)
            est = estado_instrumento(instr)
            if est['connected']:
                log.info(f"✓ Conexión establecida en intento {intento}")
                return instr
        except Exception as e:
            log.warning(f"⚠ Fallo intento {intento}: {e}")
        # Espera exponencial con jitter
        delay = base_delay * (2 ** (intento - 1)) + random.uniform(0, 0.5)
        log.info(f"Reintentando en {delay:.1f} segundos...")
        time.sleep(delay)
    raise PalmSensConnectionError("No se pudo conectar tras múltiples intentos.")


def ejecutar_sesion_remota_segura(serial: str, method_params: dict, gui_refresh_callback=None):
    """
    Igual que ejecutar_sesion_remota, pero con:
    - Conexión robusta (reintentos)
    - Validación de estado antes de medir
    - Manejo seguro de errores
    """
    conn = None
    instrumento = None
    try:
        # 1) Conexión robusta
        instrumento = conectar_con_reintentos(serial=serial)

        # 2) Validar estado antes de medir
        est = estado_instrumento(instrumento)
        if not est['connected']:
            raise PalmSensConnectionError("Instrumento no está en estado conectado.")

        # 3) Medición remota
        datos = iniciar_medicion_cv_remota(instrumento, method_params)

        # 4) Guardar en BD
        conn = get_connection()
        session_id = guardar_sesion(conn,
                                    filename=f"REMOTE_{serial}",
                                    info=datos['session_info'])
        guardar_mediciones(conn, session_id, datos['measurements'])
        log.info(f"✓ Sesión remota guardada en BD con ID {session_id}")

        # 5) Refrescar GUI
        if gui_refresh_callback:
            gui_refresh_callback()
            log.info("✓ GUI refrescada tras inserción de sesión remota")

        return session_id

    except Exception:
        log.exception("✗ Error en sesión remota segura")
        raise
    finally:
        if instrumento:
            try:
                desconectar_instrumento(instrumento)
            except Exception:
                log.warning("⚠ Fallo al desconectar instrumento")
        if conn:
            conn.close()

# ===================================================================================
# BLOQUE 7: INTEGRACIÓN CON GUI
# ===================================================================================

def ejecutar_sesion_remota_gui(serial: str,
                               method_params: dict,
                               gui_refresh_callback=None,
                               on_connect=None,
                               on_disconnect=None):
    """
    Igual que ejecutar_sesion_remota_segura, pero con hooks para GUI:
    - on_connect: se llama al conectar
    - on_disconnect: se llama al desconectar
    - gui_refresh_callback: se llama tras guardar en BD
    """
    conn = None
    instrumento = None
    try:
        instrumento = conectar_con_reintentos(serial=serial)

        if on_connect:
            on_connect(estado_instrumento(instrumento))

        datos = iniciar_medicion_cv_remota(instrumento, method_params)

        conn = get_connection()
        session_id = guardar_sesion(conn,
                                    filename=f"REMOTE_{serial}",
                                    info=datos['session_info'])
        guardar_mediciones(conn, session_id, datos['measurements'])
        log.info(f"✓ Sesión remota guardada en BD con ID {session_id}")

        if gui_refresh_callback:
            gui_refresh_callback()

        return session_id

    except Exception:
        log.exception("✗ Error en sesión remota con GUI")
        raise
    finally:
        if instrumento:
            try:
                desconectar_instrumento(instrumento)
                if on_disconnect:
                    on_disconnect()
            except Exception:
                log.warning("⚠ Fallo al desconectar instrumento")
        if conn:
            conn.close()            
