#!/bin/bash
  
sudo -u postgres psql << EOF
create user tester with password 'password';
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
psql -d test_dispatcher -U tester -a -f dispatcher.sql
psql -d test_eva -U tester -a -f eva.sql

