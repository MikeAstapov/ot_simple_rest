#!/bin/bash
  
sudo -u postgres psql << EOF
create user tester with password 'password';
EOF

sudo -u postgres psql << EOF
CREATE USER superuser_tester WITH SUPERUSER PASSWORD 'password';
EOF

sudo -u postgres psql << EOF
create database test_dispatcher;
create database test_eva;
EOF

sudo -u postgres psql << EOF
grant all privileges on database test_dispatcher to tester;
grant all privileges on database test_eva to tester;
EOF

export PGPASSWORD='password'

psql -h localhost -d test_dispatcher -U superuser_tester -a -f tests/rest/create_pgcrypto_extention.sql
psql -h localhost -d test_eva -U superuser_tester -a -f tests/rest/create_pgcrypto_extention.sql

psql -h localhost -d test_dispatcher -U tester -a -f tests/rest/dispatcher.sql
psql -h localhost -d test_eva -U tester -a -f tests/rest/eva.sql
psql -h localhost -d test_eva -U tester -a -f tests/rest/eva_quiz.sql

