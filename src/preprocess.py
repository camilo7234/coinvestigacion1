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


def normalize_for_pca(X, *, baseline_vector=None, scaler=None, method='use_trained_scaler', return_scaler=False):
    """
    Normaliza matriz X para PCA según el método solicitado.

    Parámetros:
      - X: array-like (n_samples, n_features)
      - baseline_vector: vector 1D de longitud <= n_features (se pad/trunca si es necesario)
      - scaler: objeto tipo sklearn scaler (usado si method=='use_trained_scaler')
      - method: 'use_trained_scaler' | 'zscore_columns' | 'center_only'
      - return_scaler: si True y method='use_trained_scaler' devuelve también el scaler

    Retorna: (X_normalized, meta) o (X_normalized, meta, scaler) si return_scaler True
    """
    X = np.array(X, dtype=float)
    if X.ndim == 1:
        X = X.reshape(1, -1)

    n_samples, n_features = X.shape
    meta = {'method': method, 'used_baseline': False}

    # Baseline handling: pad or truncate baseline_vector to match n_features
    if baseline_vector is not None:
        bv = np.array(baseline_vector, dtype=float).reshape(-1)
        if bv.shape[0] < n_features:
            # pad with zeros
            bv = np.concatenate([bv, np.zeros(n_features - bv.shape[0], dtype=float)])
        elif bv.shape[0] > n_features:
            bv = bv[:n_features]
        X = X - bv.reshape(1, -1)
        meta['used_baseline'] = True
        meta['baseline_length'] = int(len(bv))

    if method == 'zscore_columns':
        # Column-wise z-score
        mean = X.mean(axis=0)
        std = X.std(axis=0)
        # avoid division by zero
        std_fixed = np.where(std == 0, 1.0, std)
        Xs = (X - mean.reshape(1, -1)) / std_fixed.reshape(1, -1)
        meta.update({'mean': mean.tolist(), 'scale': std_fixed.tolist()})
        meta['method'] = 'zscore_columns'
        if return_scaler:
            scl = StandardScaler(with_mean=True, with_std=True)
            scl.mean_ = mean
            scl.scale_ = std_fixed
            return Xs, meta, scl
        return Xs, meta

    if method == 'use_trained_scaler':
        if scaler is None:
            raise ValueError("Scaler requerido para method='use_trained_scaler'")
        Xs = scaler.transform(X)
        meta['method'] = 'use_trained_scaler'
        meta['scaler_mean'] = getattr(scaler, 'mean_', None)
        meta['scaler_scale'] = getattr(scaler, 'scale_', None)
        if return_scaler:
            return Xs, meta, scaler
        return Xs, meta

    if method == 'center_only':
        mean = X.mean(axis=0)
        Xs = X - mean.reshape(1, -1)
        meta['method'] = 'center_only'
        meta.update({'mean': mean.tolist()})
        return Xs, meta

    # fallback: return as-is with meta
    meta['method'] = 'none'
    return X, meta
