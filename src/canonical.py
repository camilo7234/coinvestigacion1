"""
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

CANONICAL_LABELS = {
    'SEGURA': 'SEGURA',
    'ANOMALA': 'ANOMALA',
    'CONTAMINADA': 'CONTAMINADA'
}

# Mapeo a etiquetas de visualización (amistosas, con acento si se desea)
DISPLAY_LABELS: Dict[str, str] = {
    'SEGURA': 'Segura',
    'ANOMALA': 'Anómala',
    'CONTAMINADA': 'Contaminada'
}

# Mapeo a grupos numéricos usados en la BD (ej: 0=SEGURA, 1=ANOMALA, 2=CONTAMINADA)
GROUP_MAP: Dict[str, int] = {
    'SEGURA': 0,
    'ANOMALA': 1,
    'CONTAMINADA': 2
}


def _normalize_text(s: str) -> str:
    if s is None:
        return ''
    # Normalización simple: bajar a ASCII-equivalente, quitar emojis y espacios
    try:
        import unicodedata
        s2 = str(s)
        s2 = s2.strip()
        # NFKD + remove diacritics
        s2 = ''.join(ch for ch in unicodedata.normalize('NFKD', s2) if not unicodedata.combining(ch))
        s2 = s2.upper()
        return s2
    except Exception:
        return str(s).upper()


def normalize_classification(raw_label: str) -> str:
    """Devuelve la etiqueta canónica a partir de raw_label.

    Acepta variantes con acentos, minúsculas, emojis o texto libre y
    mapea a una de las cadenas en CANONICAL_LABELS. Si no se reconoce,
    devuelve 'SEGURA' por defecto (política conservadora).
    """
    txt = _normalize_text(raw_label)
    # simplificaciones comunes
    if not txt:
        return CANONICAL_LABELS['SEGURA']

    # Búsqueda por palabras clave
    if 'CONTAMIN' in txt or 'C' == txt[:1] and 'CONTAMIN' in txt:
        return CANONICAL_LABELS['CONTAMINADA']
    if 'ANOMAL' in txt or 'ANOM' in txt or 'ANÓM' in txt:
        return CANONICAL_LABELS['ANOMALA']
    if 'SEGUR' in txt or 'OK' == txt or 'SAFE' in txt:
        return CANONICAL_LABELS['SEGURA']

    # Catch known labels without accent
    for c in CANONICAL_LABELS.values():
        if c in txt:
            return c

    # Fallback: si contiene caracteres típicos de emoji o texto enriquecido,
    # intentar remover no ASCII y re-evaluar
    s_ascii = ''.join(ch for ch in txt if ord(ch) < 128)
    for c in CANONICAL_LABELS.values():
        if c in s_ascii:
            return c

    # Default conservador
    return CANONICAL_LABELS['SEGURA']


def classification_group_from_label(label: str) -> int:
    lab = label if label in GROUP_MAP else normalize_classification(label)
    return GROUP_MAP.get(lab, 0)


def display_label_from_label(label: str) -> str:
    lab = label if label in DISPLAY_LABELS else normalize_classification(label)
    return DISPLAY_LABELS.get(lab, DISPLAY_LABELS['SEGURA'])
