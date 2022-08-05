DROP TABLE CachesDL;

CREATE TABLE CachesDL (
    id INTEGER PRIMARY KEY,
    original_otl TEXT NOT NULL,
    tws INTEGER NOT NULL,
    twf INTEGER NOT NULL,
    field_extraction BOOLEAN DEFAULT false,
    preview BOOLEAN DEFAULT false,
    creating_date TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    expiring_date TIMESTAMPTZ NOT NULL,
    hashed_original_otl VARCHAR(128) NOT NULL,
    UNIQUE(hashed_original_otl, tws, twf, field_extraction, preview)
);