-- sql/create_tables.sql
-- Esquema DDL para crear tablas si no existen

CREATE TABLE IF NOT EXISTS regulations (
    id SERIAL PRIMARY KEY,
    created_at DATE,
    update_at TIMESTAMP,
    is_active BOOLEAN,
    title VARCHAR(255),
    gtype VARCHAR(50),
    entity VARCHAR(255),
    external_link TEXT,
    rtype_id INTEGER,
    summary TEXT,
    classification_id INTEGER,

    -- Restricción de idempotencia (basada en la lógica de la lambda)
    UNIQUE (title, created_at, external_link)
);

CREATE TABLE IF NOT EXISTS regulations_component (
    id SERIAL PRIMARY KEY,
    regulations_id INTEGER REFERENCES regulations(id) ON DELETE CASCADE,
    components_id INTEGER,

    UNIQUE (regulations_id, components_id)
);