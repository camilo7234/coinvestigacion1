"""
canonical.py
Módulo de utilidades para etiquetas canónicas de clasificación.

Proporciona una única fuente de verdad para las etiquetas que se persisten
en la base de datos y para los mapeos a grupos numéricos y etiquetas de
presentación en la UI.

Canonical labels (sin acentos, ASCII):
  - SEGURA
  - ANOMALA
  - CONTAMINADA

El módulo también normaliza entradas con acentos, mayúsculas/minúsculas,
textos enriquecidos con emojis o sufijos/prefijos.
"""
from typing import Dict

# ---------------------------------------------------------------------------
# Etiquetas canónicas (sin acentos, ASCII puro)
# Estas son las únicas cadenas que deben persistirse en la base de datos.
# ---------------------------------------------------------------------------
CANONICAL_LABELS: Dict[str, str] = {
    'SEGURA': 'SEGURA',
    'ANOMALA': 'ANOMALA',
    'CONTAMINADA': 'CONTAMINADA',
}

# ---------------------------------------------------------------------------
# Etiquetas de visualización (amigables para el usuario, con acento)
# ---------------------------------------------------------------------------
DISPLAY_LABELS: Dict[str, str] = {
    'SEGURA': 'Segura',
    'ANOMALA': 'Anómala',
    'CONTAMINADA': 'Contaminada',
}

# ---------------------------------------------------------------------------
# Mapeo a grupos numéricos usados en la BD.
# IMPORTANTE: Este mapeo es la fuente única de verdad.
#   0 = SEGURA
#   1 = ANOMALA
#   2 = CONTAMINADA
# db_persistence.py y query_sessions() deben usar estos mismos valores.
# ---------------------------------------------------------------------------
GROUP_MAP: Dict[str, int] = {
    'SEGURA': 0,
    'ANOMALA': 1,
    'CONTAMINADA': 2,
}

# ---------------------------------------------------------------------------
# Mapeo inverso: de grupo numérico a etiqueta canónica.
# Útil para interpretar valores que ya están en la BD.
# ---------------------------------------------------------------------------
GROUP_TO_LABEL: Dict[int, str] = {v: k for k, v in GROUP_MAP.items()}


def _normalize_text(s: str) -> str:
    """Normaliza un string a mayúsculas ASCII sin diacríticos."""
    if s is None:
        return ''
    try:
        import unicodedata
        s2 = str(s).strip()
        # NFKD + eliminar diacríticos combinantes
        s2 = ''.join(
            ch for ch in unicodedata.normalize('NFKD', s2)
            if not unicodedata.combining(ch)
        )
        return s2.upper()
    except Exception:
        return str(s).upper()


def normalize_classification(raw_label: str) -> str:
    """
    Devuelve la etiqueta canónica a partir de raw_label.

    Acepta variantes con acentos, minúsculas, emojis o texto libre y
    mapea a una de las cadenas en CANONICAL_LABELS.

    CORRECCIÓN (bug anterior): La condición
        'CONTAMIN' in txt or 'C' == txt[:1] and 'CONTAMIN' in txt
    tenía la segunda parte completamente redundante debido a la
    precedencia de 'and' sobre 'or' en Python. Se simplificó a
    condiciones independientes y sin ambigüedad.

    Política de fallback: devuelve 'SEGURA' si no se reconoce la entrada
    (política conservadora — nunca eleva la clasificación por error).
    """
    txt = _normalize_text(raw_label)

    if not txt:
        return CANONICAL_LABELS['SEGURA']

    # Evaluar en orden de mayor a menor gravedad para evitar
    # que 'SEGURA' capture antes que 'CONTAMINADA' o 'ANOMALA'.

    # 1. CONTAMINADA
    if 'CONTAMIN' in txt:
        return CANONICAL_LABELS['CONTAMINADA']

    # 2. ANOMALA
    if 'ANOMAL' in txt or 'ANOM' in txt:
        return CANONICAL_LABELS['ANOMALA']

    # 3. SEGURA
    if 'SEGUR' in txt or txt == 'OK' or 'SAFE' in txt:
        return CANONICAL_LABELS['SEGURA']

    # 4. Comparación directa con etiquetas canónicas conocidas
    for canonical in CANONICAL_LABELS.values():
        if canonical in txt:
            return canonical

    # 5. Fallback eliminando caracteres no ASCII (emojis, símbolos)
    s_ascii = ''.join(ch for ch in txt if ord(ch) < 128)
    for canonical in CANONICAL_LABELS.values():
        if canonical in s_ascii:
            return canonical

    # 6. Default conservador
    return CANONICAL_LABELS['SEGURA']


def classification_group_from_label(label: str) -> int:
    """
    Devuelve el grupo numérico (0, 1 ó 2) correspondiente a la etiqueta.
    Normaliza la etiqueta si no es canónica antes de buscar en GROUP_MAP.
    Valor por defecto: 0 (SEGURA) — política conservadora.
    """
    canonical = label if label in GROUP_MAP else normalize_classification(label)
    return GROUP_MAP.get(canonical, 0)


def display_label_from_label(label: str) -> str:
    """
    Devuelve la etiqueta de visualización amigable (con tilde si aplica).
    Normaliza la etiqueta si no es canónica antes de buscar en DISPLAY_LABELS.
    """
    canonical = label if label in DISPLAY_LABELS else normalize_classification(label)
    return DISPLAY_LABELS.get(canonical, DISPLAY_LABELS['SEGURA'])


def label_from_group(group: int) -> str:
    """
    Convierte un grupo numérico de la BD a la etiqueta canónica.
    Útil al leer registros de la BD y necesitar la etiqueta de texto.
    Valor por defecto: 'SEGURA' si el grupo no existe en el mapa.
    """
    return GROUP_TO_LABEL.get(group, 'SEGURA')
