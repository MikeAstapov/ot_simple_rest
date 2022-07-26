#!/bin/bash

sudo -u postgres psql << EOF
drop database eva;
drop database dispatcher;
drop user dispatcher;
drop user superuser_dispatcher;
EOF
