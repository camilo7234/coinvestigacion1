import sys, traceback
from pathlib import Path
repo_root = Path(__file__).resolve().parents[1]
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))

import numpy as np
from src.preprocess import normalize_for_pca

failures = []

# Test 1: zscore columns basic
try:
    X = np.array([[1.0,2.0,3.0],[2.0,4.0,6.0]])
    Xs, meta = normalize_for_pca(X, method='zscore_columns')
    assert 'method' in meta and meta['method'] == 'zscore_columns'
    means = np.mean(Xs, axis=0)
    assert np.allclose(means, np.zeros_like(means), atol=1e-7)
    print('test_zscore_columns_basic: PASS')
except Exception as e:
    print('test_zscore_columns_basic: FAIL')
    traceback.print_exc()
    failures.append('zscore')

# Test 2: use_trained_scaler requires scaler
try:
    X = np.array([[1.0,2.0]])
    try:
        normalize_for_pca(X, method='use_trained_scaler')
        print('test_use_trained_scaler_requires_scaler: FAIL (no exception)')
        failures.append('scaler_no_exception')
    except ValueError:
        print('test_use_trained_scaler_requires_scaler: PASS')
except Exception:
    print('test_use_trained_scaler_requires_scaler: FAIL')
    traceback.print_exc()
    failures.append('scaler')

# Test 3: baseline padding/truncation
try:
    X = np.array([[1.0,2.0,3.0]])
    baseline_short = np.array([0.1,0.1])
    Xs, meta = normalize_for_pca(X, baseline_vector=baseline_short, method='zscore_columns')
    assert meta.get('used_baseline') is True
    assert Xs.shape == (1,3)
    print('test_baseline_padding_truncation: PASS')
except Exception:
    print('test_baseline_padding_truncation: FAIL')
    traceback.print_exc()
    failures.append('baseline')

if failures:
    print('\nSOME TESTS FAILED:', failures)
    sys.exit(2)
else:
    print('\nALL TESTS PASSED')
    sys.exit(0)
