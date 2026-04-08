-- schema.sql

-- 1) Tabla de sesiones
CREATE TABLE IF NOT EXISTS sessions (
    id                 SERIAL PRIMARY KEY,
    filename           TEXT    NOT NULL,        -- Nombre del archivo .pssession
    loaded_at          TIMESTAMP NOT NULL,      -- Fecha y hora de carga
    scan_rate          DOUBLE PRECISION,        -- Frecuencia de escaneo (Hz)
    start_potential    DOUBLE PRECISION,        -- Potencial inicial (V)
    end_potential      DOUBLE PRECISION,        -- Potencial final (V)
    software_version   TEXT                     -- Versión del software PalmSens
);

-- 2) Tabla de mediciones (measurements)
CREATE TABLE IF NOT EXISTS measurements (
    id                    SERIAL PRIMARY KEY,
    session_id            INTEGER NOT NULL
                             REFERENCES sessions(id) ON DELETE CASCADE,
    title                 TEXT    NOT NULL,
    timestamp             TIMESTAMP NOT NULL,
    device_serial         TEXT    NOT NULL,
    curve_count           INTEGER NOT NULL
    -- Las siguientes columnas las añadiremos por ALTER más abajo:
    -- pca_data DOUBLE PRECISION[] NOT NULL,
    -- ppm_estimations DOUBLE PRECISION[]
);

-- 3) Tabla de curvas (curves)
CREATE TABLE IF NOT EXISTS curves (
    id             SERIAL PRIMARY KEY,
    measurement_id INTEGER NOT NULL
                   REFERENCES measurements(id) ON DELETE CASCADE,
    curve_index    INTEGER NOT NULL,
    num_points     INTEGER NOT NULL
);

-- 4) Tabla de puntos (points)
CREATE TABLE IF NOT EXISTS points (
    id        SERIAL PRIMARY KEY,
    curve_id  INTEGER NOT NULL
             REFERENCES curves(id) ON DELETE CASCADE,
    potential DOUBLE PRECISION NOT NULL,
    current   DOUBLE PRECISION NOT NULL
);

-- Índices adicionales para optimizar consultas
CREATE INDEX IF NOT EXISTS idx_measurements_session ON measurements(session_id);
CREATE INDEX IF NOT EXISTS idx_curves_measurement ON curves(measurement_id);
CREATE INDEX IF NOT EXISTS idx_points_curve ON points(curve_id);

-- 5) Modificaciones mínimas a measurements
ALTER TABLE measurements
  DROP COLUMN IF EXISTS pca_data,
  DROP COLUMN IF EXISTS ppm_estimations,
  ADD COLUMN IF NOT EXISTS pca_scores DOUBLE PRECISION[],         -- Scores PCA para clasificación
  ADD COLUMN IF NOT EXISTS classification_group SMALLINT,         -- Grupo: 0=Sin metales, 1=Con metales, 2=Anómalo
  ADD COLUMN IF NOT EXISTS contamination_level DOUBLE PRECISION;  -- Nivel de contaminación 0–100%

-- (Opcional) Trigger para mantener updated_at en measurements
ALTER TABLE measurements
  ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW();

CREATE OR REPLACE FUNCTION trg_set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = NOW();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS measurements_update_trigger ON measurements;
CREATE TRIGGER measurements_update_trigger
  BEFORE UPDATE ON measurements
  FOR EACH ROW
  EXECUTE PROCEDURE trg_set_updated_at();
