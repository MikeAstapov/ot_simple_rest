CREATE TYPE status_type AS ENUM ('new', 'running', 'failed', 'finished', 'external', 'canceled');

CREATE TABLE SplQueries (
    id SERIAL PRIMARY KEY,
    original_spl TEXT NOT NULL,
    service_spl TEXT NOT NULL,
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
    original_spl TEXT NOT NULL,
    tws INTEGER NOT NULL,
    twf INTEGER NOT NULL,
    field_extraction BOOLEAN DEFAULT false,
    preview BOOLEAN DEFAULT false,
    creating_date TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    expiring_date TIMESTAMPTZ NOT NULL,
    UNIQUE(original_spl, tws, twf, field_extraction, preview)
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

CREATE TABLE SplunkSIDs (
    sid TEXT NOT NULL,
    src_ip TEXT NOT NULL,
    spl TEXT NOT NULL,
    UNIQUE(sid, src_ip)
);

CREATE TABLE Ticks (
    applicationId TEXT PRIMARY KEY,
    lastCheck TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
