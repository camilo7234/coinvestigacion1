import numpy as np
import pytest
from preprocess import normalize_for_pca
from sklearn.preprocessing import StandardScaler


def test_zscore_columns_basic():
    X = np.array([[1.0, 2.0, 3.0], [2.0, 4.0, 6.0]])
    Xs, meta = normalize_for_pca(X, method='zscore_columns')
    # Column-wise zscore: mean [1.5,3.0,4.5], std [0.5,1.0,1.5]
    assert 'method' in meta and meta['method'] == 'zscore_columns'
    assert pytest.approx(np.mean(Xs, axis=0), abs=1e-7) == np.array([0.0, 0.0, 0.0])


def test_use_trained_scaler_requires_scaler():
    X = np.array([[1.0, 2.0]])
    with pytest.raises(ValueError):
        normalize_for_pca(X, method='use_trained_scaler')


def test_baseline_padding_truncation():
    X = np.array([[1.0, 2.0, 3.0]])
    baseline_short = np.array([0.1, 0.1])
    Xs, meta = normalize_for_pca(X, baseline_vector=baseline_short, method='zscore_columns')
    # baseline_short should be padded to length 3, and used_baseline True
    assert meta.get('used_baseline') is True
    # Check that result has same shape
    assert Xs.shape == (1, 3)
