#!/bin/bash

sudo -u postgres psql << EOF
drop database eva;
drop database dispatcher;
EOF
