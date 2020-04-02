#!/bin/bash

sudo -u postgres psql << EOF
create user dispatcher with password 'password';
create database eva;
create database dispatcher;
grant all privileges on database eva to dispatcher;
grant all privileges on database dispatcher to dispatcher;
EOF

#export PGPASSWORD='P@$$w0rd'
psql -d eva -U dispatcher -a -f init_tables.sql
