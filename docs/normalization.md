# Normalización y trazabilidad para PCA

Resumen de la política aplicada en el pipeline (decisión experta):

1) Módulo central
- `src/canonical.py` se mantiene como fuente de verdad para etiquetas canónicas y mapeos a grupo/display.

2) Selección de datos para PCA
- Se usa el tercer ciclo (índice 2) como vector representativo por consistencia y reproducibilidad.

3) Baseline
- Solo se resta baseline si existe una baseline "certificada" (preferencia: `meta.pkl` en `models/` o `models/baseline.npy`).
- Política conservadora: NO se resta la media del propio sample si no hay baseline confiable.

4) Escalado / Normalización
- Preferido: `use_trained_scaler` (aplica `scaler.transform(X)` usando el scaler entrenado guardado en `models/scaler.pkl`).
- Alternativa segura: `zscore_columns` (z-score por característica) — útil como fallback controlado.
- Otras opciones soportadas: `normalize_row` (L2 por fila), `center_only` (restar media por columna).

5) Trazabilidad y metadatos
- Se añade y persiste en BD una columna `measurements.model_meta (JSONB)` con metadatos relevantes:
  - `model_version`, `used_n_features`, `used_baseline` (bool), `baseline_source`, `notes`, `scaler_mean`, `scaler_scale`, `norm_method`.
- También el CSV de salida incluye campos de `model_meta` por fila para auditoría.

6) No se asumen valores por defecto
- Si el método `use_trained_scaler` es solicitado pero no hay scaler disponible, el pipeline registra el evento y hace un fallback documentado a `zscore_columns`.
- Todas las decisiones de fallback se registran en `model_meta.notes`.

7) Validación con muestras patrón
- Recomendado: ejecutar validación con blanks/patrón (muestras conocidas) tras cualquier reentrenamiento o cambio de normalización. Los tests automatizados deben confirmar que los blanks quedan agrupados en PCA.

8) Recomendación operativa
- Mantener `meta.pkl` en `models/` con fields: `n_features`, `baseline` (vector), `normalization_method`.
- Versionar `models/` y registrar `model_meta.model_version` para reproducibilidad.


Archivo `tools/migrate_add_model_meta.py` disponible para añadir la columna `model_meta` a la base de datos (uso manual por administrador).

Si quieres, creo un README corto adicional o añado un ejemplo de `meta.pkl` en `models/` ahora.