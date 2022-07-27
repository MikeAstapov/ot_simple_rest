#!/bin/bash

sudo -u postgres psql << EOF
create database dispatcher;
EOF

psql -h localhost -d dispatcher -U dispatcher -a -f dispatcher.sql
