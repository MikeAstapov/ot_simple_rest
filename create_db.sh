#!/bin/bash

sudo -u postgres psql << EOF
create database eva;
EOF

#sudo -u postgres psql << EOF
#create user tester with encrypted password '12345678';
#EOF

sudo -u postgres psql << EOF
grant all privileges on database eva to dispatcher;
EOF

export PGPASSWORD='P@$$w0rd'
psql -d eva -U dispatcher -a -f init_tables.sql
