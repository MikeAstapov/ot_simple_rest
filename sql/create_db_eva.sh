#!/bin/bash

sudo -u postgres psql << EOF
create database eva;
grant all privileges on database eva to dispatcher;
EOF

psql -h localhost -d eva -U superuser_dispatcher -a -f create_pgcrypto_extention.sql
psql -h localhost -d eva -U dispatcher -a -f eva.sql
psql -h localhost -d eva -U dispatcher -a -f eva_quiz.sql
