#!/bin/bash

sudo -u postgres psql << EOF
create database dispatcher;
grant all privileges on database dispatcher to dispatcher;
EOF

psql -h localhost -d dispatcher -U superuser_dispatcher -a -f create_pgcrypto_extention.sql
psql -h localhost -d dispatcher -U dispatcher -a -f dispatcher.sql
