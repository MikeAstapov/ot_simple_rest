CREATE TABLE "user" (
    id SERIAL PRIMARY KEY,
    name VARCHAR(512) NOT NULL UNIQUE,
    password VARCHAR(512) NOT NULL
);

CREATE TABLE role (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL UNIQUE
);

CREATE TABLE dash (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    body TEXT
);

CREATE TABLE permission (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL UNIQUE
);

CREATE TABLE "group" (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL UNIQUE,
    color VARCHAR(100) NOT NULL
);

CREATE TABLE index (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL UNIQUE
);

CREATE TABLE session (
    id SERIAL PRIMARY KEY,
    token TEXT UNIQUE,
    user_id INT NOT NULL REFERENCES "user"(id) ON DELETE CASCADE,
    expired_date TIMESTAMPTZ NOT NULL
);

CREATE TABLE user_role (
    user_id INT NOT NULL REFERENCES "user"(id) ON DELETE CASCADE,
    role_id INT NOT NULL REFERENCES role(id) ON DELETE CASCADE,
    CONSTRAINT user_role_id UNIQUE(user_id, role_id)
);

CREATE TABLE index_group (
    index_id INT NOT NULL REFERENCES index(id) ON DELETE CASCADE,
    group_id INT NOT NULL REFERENCES "group"(id) ON DELETE CASCADE,
    CONSTRAINT index_group_id UNIQUE(index_id, group_id)
);

CREATE TABLE user_group (
    user_id INT NOT NULL REFERENCES "user"(id) ON DELETE CASCADE,
    group_id INT NOT NULL REFERENCES "group"(id) ON DELETE CASCADE,
    CONSTRAINT user_group_id UNIQUE(user_id, group_id)
);

CREATE TABLE dash_group (
    dash_id INT NOT NULL REFERENCES dash(id) ON DELETE CASCADE,
    group_id INT NOT NULL REFERENCES "group"(id) ON DELETE CASCADE,
    CONSTRAINT dash_group_id UNIQUE(dash_id, group_id)
);

CREATE TABLE role_permission (
    permission_id INT NOT NULL REFERENCES permission(id) ON DELETE CASCADE,
    role_id INT NOT NULL REFERENCES role(id) ON DELETE CASCADE,
    value BOOLEAN,
    CONSTRAINT role_permission_id UNIQUE(permission_id, role_id)
);


-- ###### QUIZ ######

CREATE TABLE quiz (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL
);

CREATE TABLE question (
    id SERIAL PRIMARY KEY,
    text TEXT,
    type VARCHAR(100) NOT NULL
);

CREATE TABLE quiz_question (
    quiz_id INT NOT NULL REFERENCES quiz(id) ON DELETE CASCADE,
    question_id INT NOT NULL REFERENCES question(id) ON DELETE CASCADE,
    sid INT NOT NULL,
    CONSTRAINT quiz_question_id UNIQUE(quiz_id, question_id)
);

CREATE TABLE boolAnswer (
    id INT NOT NULL REFERENCES question(id) ON DELETE CASCADE,
    value BOOLEAN,
    description TEXT
);

CREATE TABLE textAnswer (
    id INT NOT NULL REFERENCES question(id) ON DELETE CASCADE,
    description TEXT
);

CREATE TABLE dataAnswer(
    id INT NOT NULL REFERENCES question(id) ON DELETE CASCADE,
    date timestamptz DEFAULT CURRENT_TIMESTAMP
);


INSERT INTO "user" (name, password) VALUES ('admin', '$2b$12$ODxOO2wd6vRy2wT4euCJxeKPwU7.GW7HvTrcFmCQgTFbMQOfj851e');
INSERT INTO permission (name) VALUES ('admin_all');
INSERT INTO role (name) VALUES ('admin');
INSERT INTO user_role (user_id, role_id) VALUES (1, 1);
INSERT INTO role_permission (role_id, permission_id) VALUES (1, 1);
