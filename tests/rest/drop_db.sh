#!/bin/bash
  
sudo -u postgres psql << EOF
REASSIGN OWNED BY tester TO postgres;
DROP OWNED BY tester;
drop database test_dispatcher;
drop database test_eva;
drop user tester;
drop user superuser_tester;
EOF
