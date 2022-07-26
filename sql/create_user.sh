#!/bin/bash

sudo -u postgres psql << EOF
create user dispatcher with password 'password';
CREATE USER superuser_dispatcher WITH SUPERUSER PASSWORD 'password';
EOF

