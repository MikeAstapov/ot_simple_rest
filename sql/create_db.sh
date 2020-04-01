#!/bin/bash

sudo -u postgres psql << EOF
create user dispatcher with password 'password';
EOF

sudo -u postgres psql << EOF
create database eva;
create database dispatcher;
EOF

#sudo -u postgres psql << EOF
#create user tester with encrypted password '12345678';
#EOF

sudo -u postgres psql << EOF
grant all privileges on database eva to dispatcher;
grant all privileges on database dispatcher to dispatcher;
EOF

export PGPASSWORD='P@$$w0rd'
psql -d eva -U dispatcher -a -f init_tables.sql
