-- ###### QUIZ ######
CREATE TYPE multi_str AS ENUM ('yes', 'no', 'skip');

CREATE TABLE quiz (
    id SERIAL PRIMARY KEY,
    name VARCHAR(512) NOT NULL
);

CREATE TABLE filled_quiz (
    id SERIAL PRIMARY KEY,
    quiz_id INT REFERENCES quiz(id) ON DELETE CASCADE,
    filler VARCHAR(256),
    fill_date timestamptz DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE catalog (
    id SERIAL PRIMARY KEY,
    name VARCHAR (256),
    content TEXT
);

CREATE TABLE question (
    id SERIAL PRIMARY KEY,
    description TEXT,
    quiz_id INT REFERENCES quiz(id) ON DELETE CASCADE,
    sid INT,
    text TEXT NOT NULL,
    type VARCHAR(100) NOT NULL,
    is_sign BOOLEAN DEFAULT FALSE,
    label VARCHAR(256),
    parent_id INT REFERENCES question(id) ON DELETE CASCADE,
    catalog_id INT REFERENCES catalog(id) ON DELETE CASCADE
);

CREATE TABLE boolAnswer (
    id INT NOT NULL REFERENCES filled_quiz(id) ON DELETE CASCADE,
    sid INT NOT NULL,
    value BOOLEAN,
    description TEXT
);

CREATE TABLE multiAnswer (
    id INT NOT NULL REFERENCES filled_quiz(id) ON DELETE CASCADE,
    sid INT NOT NULL,
    value multi_str,
    description TEXT
);

CREATE TABLE textAnswer (
    id INT NOT NULL REFERENCES filled_quiz(id) ON DELETE CASCADE,
    sid INT NOT NULL,
    value TEXT,
    description TEXT
);

CREATE TABLE dateAnswer(
    id INT NOT NULL REFERENCES filled_quiz(id) ON DELETE CASCADE,
    sid INT NOT NULL,
    value timestamptz DEFAULT CURRENT_TIMESTAMP,
    description TEXT
);

CREATE TABLE cascadeAnswer(
    id INT NOT NULL REFERENCES filled_quiz(id) ON DELETE CASCADE,
    sid INT NOT NULL,
    value TEXT,
    description TEXT
);

CREATE TABLE catalogAnswer(
    id INT NOT NULL REFERENCES filled_quiz(id) ON DELETE CASCADE,
    sid INT NOT NULL,
    value TEXT,
    description TEXT
);

