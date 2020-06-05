#!/bin/bash

sudo -u postgres psql << EOF
create user dispatcher with password 'password';
EOF

