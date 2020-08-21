INSERT INTO permission (name) VALUES ('editdash');
INSERT INTO permission (name) VALUES ('managedash');
INSERT INTO permission (name) VALUES ('managegroup');
INSERT INTO role (name) VALUES ('editdash_role');
INSERT INTO role (name) VALUES ('managedash_role');
INSERT INTO role (name) VALUES ('managegroup_role');

INSERT INTO role_permission (role_id, permission_id) VALUES (5, 5);
INSERT INTO role_permission (role_id, permission_id) VALUES (6, 6);
INSERT INTO role_permission (role_id, permission_id) VALUES (7, 7);