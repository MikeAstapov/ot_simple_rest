#!/bin/bash

sudo -u postgres psql << EOF
create database eva;
grant all privileges on database eva to dispatcher;
EOF

psql -d eva -U dispatcher -a -f eva.sql
