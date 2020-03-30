#!/bin/bash

sudo -u postgres psql << EOF
drop database eva;
EOF

#sudo -u postgres psql << EOF
#drop user tester;
#EOF
