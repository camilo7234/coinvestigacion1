# src/preprocess.py
"""
Funciones de preprocesamiento mínimas: baseline subtraction y scaler helpers.
"""

import numpy as np
import joblib
from sklearn.preprocessing import StandardScaler

def baseline_subtract(matrix, baseline_vector=None, method='col_min'):
    """
    matrix: np.ndarray (n_samples, n_points)
    baseline_vector: np.ndarray (n_points,) - si la tienes (blanco)
    method: 'col_min' | 'row_mean' | 'none'
    Retorna: np.ndarray con baseline restado
    """
    X = np.array(matrix, dtype=float)
    if baseline_vector is not None:
        bv = np.array(baseline_vector, dtype=float)
        if bv.shape[0] != X.shape[1]:
            raise ValueError("baseline_vector debe tener longitud igual al número de columnas (potenciales)")
        return X - bv.reshape(1, -1)
    if method == 'col_min':
        return X - X.min(axis=0, keepdims=True)
    if method == 'row_mean':
        return X - X.mean(axis=1, keepdims=True)
    return X  # 'none'

def fit_scaler(X):
    """Ajusta un StandardScaler (z-score por columna)."""
    scaler = StandardScaler(with_mean=True, with_std=True)
    scaler.fit(X)
    return scaler

def apply_scaler(X, scaler):
    return scaler.transform(X)

def save_artifact(obj, path):
    joblib.dump(obj, path)

def load_artifact(path):
    return joblib.load(path)
