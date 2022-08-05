CREATE TYPE status_type AS ENUM ('new', 'running', 'failed', 'finished', 'external', 'canceled');

CREATE TABLE OTLQueries (
    id SERIAL PRIMARY KEY,
    original_otl TEXT NOT NULL,
    service_otl TEXT NOT NULL,
    subsearches TEXT[],
    tws INTEGER NOT NULL,
    twf INTEGER NOT NULL,
    cache_ttl INTEGER NOT NULL DEFAULT 0,
    field_extraction BOOLEAN DEFAULT false,
    username TEXT NOT NULL,
    user_group_id INTEGER DEFAULT 0,
    creating_date TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    preview BOOLEAN DEFAULT false,
    status status_type DEFAULT 'new',
    msg TEXT
);

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

CREATE TABLE CachesLock (
    id INTEGER,
    locker INTEGER,
    UNIQUE(id, locker)
);

CREATE TABLE RoleModel (
    username TEXT PRIMARY KEY,
    roles TEXT[],
    indexes TEXT[]
);

CREATE TABLE DataModels (
    name TEXT PRIMARY KEY,
    search TEXT
);

CREATE TABLE GUISIDs (
    sid TEXT NOT NULL,
    src_ip TEXT NOT NULL,
    otl TEXT NOT NULL,
    UNIQUE(sid, src_ip)
);

CREATE TABLE Ticks (
    applicationId TEXT PRIMARY KEY,
    lastCheck TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
