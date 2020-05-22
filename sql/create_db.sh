#!/bin/bash

sudo -u postgres psql << EOF
create user dispatcher with password 'password';
create database eva;
create database dispatcher;
grant all privileges on database eva to dispatcher;
grant all privileges on database dispatcher to dispatcher;
EOF

psql -h localhost -d eva -U dispatcher -a -f eva.sql
psql -h localhost -d eva -U dispatcher -a -f eva_quiz.sql
