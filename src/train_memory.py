# src/train_memory.py
"""
Entrena scaler + PCA + modelo (MultiOutput) usando el Excel de referencia.
Corrige NaN tanto en X como en y, limpia columnas vacías y entrena el modelo.
"""

from pathlib import Path
import joblib
import pandas as pd
import numpy as np
from sklearn.decomposition import PCA
from sklearn.linear_model import RidgeCV
from sklearn.multioutput import MultiOutputRegressor
from sklearn.model_selection import KFold, cross_val_predict
from sklearn.metrics import mean_squared_error, r2_score

from preprocess import baseline_subtract, fit_scaler

# === CONFIGURACIÓN ===
ROOT = Path(__file__).resolve().parents[1]
XLSX = ROOT / "data" / "Datos de referencia_laboratorio, matriz_2_UNAL 191925 .xlsx"
OUTDIR = ROOT / "models"
OUTDIR.mkdir(exist_ok=True)
SHEET = "Matriz"

# === CARGA ===
print(f"📂 Leyendo hoja '{SHEET}' desde:\n{XLSX}")
df = pd.read_excel(XLSX, sheet_name=SHEET, header=None)

# detectar fila de encabezado
header_row = None
for i in range(min(10, len(df))):
    if any(isinstance(v, str) for v in df.iloc[i].values):
        header_row = i
        break
if header_row is not None:
    df = pd.read_excel(XLSX, sheet_name=SHEET, header=header_row)
else:
    df = pd.read_excel(XLSX, sheet_name=SHEET, header=0)

print("Dimensiones originales:", df.shape)
print("Primeras columnas:", list(df.columns)[:10])
print("--------------------------------------------------")

# === LIMPIEZA GENERAL ===
df = df.dropna(how='all')
df = df.applymap(lambda x: str(x).replace(",", ".") if isinstance(x, str) else x)
df = df.apply(pd.to_numeric, errors='coerce')

# eliminar columnas completamente vacías
empty_cols = df.columns[df.isna().all()].tolist()
if empty_cols:
    print(f"⚠️ Eliminando {len(empty_cols)} columnas vacías.")
    df = df.drop(columns=empty_cols)

df = df.dropna(how='all')
print("✅ Convertido todo a numérico. Dimensiones:", df.shape)

# === SEPARAR X/Y ===
cols = [str(c).lower() for c in df.columns]
target_cols = [df.columns[i] for i, c in enumerate(cols) if 'ppm' in c or 'mg/l' in c]
if not target_cols:
    target_cols = [df.columns[-1]]

exclude = set(target_cols)
cols_X = [c for c in df.columns if c not in exclude]
X = df[cols_X].values
y = df[target_cols].values

print("Detectados targets:", target_cols)
print("X shape:", X.shape, "y shape:", y.shape)

# === LIMPIEZA ADICIONAL EN Y ===
print("Revisando y corrigiendo NaN en y...")
if np.isnan(y).any():
    nan_ratio_y = np.isnan(y).sum() / y.size * 100
    print(f"⚠️ Se detectaron NaN en {nan_ratio_y:.2f}% de los valores de laboratorio. Eliminando filas incompletas...")

    # eliminamos las filas donde falten valores en y
    valid_rows = ~np.isnan(y).any(axis=1)
    X = X[valid_rows]
    y = y[valid_rows]

print("✔️ Sin NaN restantes en y:", not np.isnan(y).any())
print(f"Datos finales tras limpieza → X: {X.shape}, y: {y.shape}")
print("--------------------------------------------------")

# === VALIDACIÓN Y CORRECCIÓN DE NAN EN X ===
print("Revisando y corrigiendo NaN en X...")
if np.isnan(X).any():
    nan_ratio = np.isnan(X).sum() / X.size * 100
    print(f"⚠️ Se detectaron NaN en {nan_ratio:.2f}% de los datos. Corrigiendo...")
    # eliminar columnas completamente vacías
    all_nan_cols = np.all(np.isnan(X), axis=0)
    if np.any(all_nan_cols):
        print(f"  → Eliminando {np.sum(all_nan_cols)} columnas completamente vacías.")
        X = X[:, ~all_nan_cols]

    # rellenar con promedios de columna
    col_means = np.nanmean(X, axis=0)
    inds = np.where(np.isnan(X))
    X[inds] = np.take(col_means, inds[1])

# reemplazo final de NaN con 0
X = np.nan_to_num(X, nan=0.0)
print("✔️ Sin NaN restantes en X:", not np.isnan(X).any())
print("--------------------------------------------------")

# === NORMALIZACIÓN Y PCA ===
print("Aplicando baseline subtraction...")
X_bs = baseline_subtract(X, baseline_vector=None, method='col_min')

print("Aplicando scaler...")
scaler = fit_scaler(X_bs)
Xs = scaler.transform(X_bs)
joblib.dump(scaler, OUTDIR / "scaler.pkl")
print("Scaler guardado ✅")

print("Ejecutando PCA...")
pca = PCA(n_components=0.99, svd_solver='full')
pca.fit(Xs)
Xp = pca.transform(Xs)
joblib.dump(pca, OUTDIR / "pca.pkl")
print(f"PCA guardado ✅  n_components: {pca.n_components_}")

# === ENTRENAMIENTO ===
print("Entrenando modelo RidgeCV...")
base = RidgeCV(alphas=[0.1, 1.0, 10.0], cv=5)
model = MultiOutputRegressor(base)
model.fit(Xp, y)
joblib.dump(model, OUTDIR / "model.pkl")
print("Modelo guardado ✅")

# === VALIDACIÓN CRUZADA ===
kf = KFold(n_splits=5, shuffle=True, random_state=42)
y_pred = cross_val_predict(model, Xp, y, cv=kf)
rmse = np.sqrt(mean_squared_error(y, y_pred))
r2 = r2_score(y, y_pred)
print(f"Validación cruzada → RMSE: {rmse:.4f} | R2: {r2:.4f}")

# === METADATOS ===
meta = {
    "targets": [str(t) for t in target_cols],
    "n_features": X.shape[1],
    "pca_n_components": pca.n_components_,
}
joblib.dump(meta, OUTDIR / "meta.pkl")
print("Meta guardada ✅")

print("\n🎯 Entrenamiento completado correctamente. Modelos en:", OUTDIR)
