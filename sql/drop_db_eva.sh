#!/bin/bash

sudo -u postgres psql << EOF
drop database eva;
EOF
