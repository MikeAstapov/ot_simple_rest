-- ###### QUIZ ######

CREATE TABLE quiz (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL
);

CREATE TABLE filled_quiz (
    id SERIAL PRIMARY KEY,
    quiz_id INT REFERENCES quiz(id) ON DELETE CASCADE,
    name VARCHAR(100) NOT NULL
);

CREATE TABLE question (
    id SERIAL PRIMARY KEY,
    quiz_id INT REFERENCES quiz(id) ON DELETE CASCADE,
    filled_quiz_id INT REFERENCES filled_quiz(id) ON DELETE CASCADE,
    sid INT NOT NULL,
    text TEXT NOT NULL,
    type VARCHAR(100) NOT NULL,
    is_sign BOOLEAN DEFAULT FALSE,
    label VARCHAR(100)
);

CREATE TABLE boolAnswer (
    id INT NOT NULL REFERENCES question(id) ON DELETE CASCADE,
    value BOOLEAN,
    description TEXT
);

CREATE TABLE textAnswer (
    id INT NOT NULL REFERENCES question(id) ON DELETE CASCADE,
    value TEXT,
    description TEXT
);

CREATE TABLE dateAnswer(
    id INT NOT NULL REFERENCES question(id) ON DELETE CASCADE,
    value timestamptz DEFAULT CURRENT_TIMESTAMP,
    description TEXT
);
