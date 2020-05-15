
#.SILENT:

COMPONENTS := venv ot_simple_rest.pth tests.pth start.sh stop.sh ot_simple_rest.conf nginx sql

TESTS_PTH = /tests
OT_REST_PTH = /ot_simple_rest
BASE_PTH = $(shell pwd)

all:
	echo -e "Required section:\n\
 build - build project into build directory, with configuration file and enveroment\n\
 clean - clean all addition file, build directory and output archive file\n\
 test - run all tests ([unit test], [rest api test], [cluster mode test...])\n\
 pack - make output archive, file name format \"ot_simple_rest_vX.Y.Z_BRANCHNAME.tar.gz\"\n\
Addition section:\n\
 venv\n\
 start.sh\n\
 stop.sh\n\
 nginx\n\
"

GENERATE_VERSION = $(shell cat ot_simple_rest/ot_simple_rest.py | grep __version__ | head -n 1 | sed -re 's/[^"]+//' | sed -re 's/"//g' )
GENERATE_BRANCH = $(shell git name-rev $$(git rev-parse HEAD) | cut -d\  -f2 | sed -re 's/^(remotes\/)?origin\///' | tr '/' '_')
SET_VERSION = $(eval VERSION=$(GENERATE_VERSION))
SET_BRANCH = $(eval BRANCH=$(GENERATE_BRANCH))

pack: build
	$(SET_VERSION)
	$(SET_BRANCH)
	rm -f ot_simple_rest-*.tar.gz
	echo Create archive \"ot_simple_rest-$(VERSION)-$(BRANCH).tar.gz\"
	cd build; tar czf ../ot_simple_rest-$(VERSION)-$(BRANCH).tar.gz ot_simple_rest nginx sql

ot_simple_rest.tar.gz: build
	cd build; tar czf ../ot_simple_rest.tar.gz ot_simple_rest nginx sql && rm -rf ../build

build: $(COMPONENTS)
	# required section
	echo Build
	mkdir build
	cp -r ot_simple_rest build
	cp -r venv build/ot_simple_rest
	cp ot_simple_rest.pth venv/lib/python3.6/site-packages
	cp tests.pth venv/lib/python3.6/site-packages
	ln -s /opt/otp/logs/ot_simple_rest build/ot_simple_rest/logs
	cp start.sh build/ot_simple_rest/start.sh
	cp stop.sh build/ot_simple_rest/stop.sh
	cp ot_simple_rest.conf build/ot_simple_rest/ot_simple_rest.conf
	cp -r nginx build
	cp -r sql build

venv:
	echo Create venv
	mkdir -p /opt/otp/ot_simple_rest
	python3 -m venv --copies /opt/otp/ot_simple_rest/venv
	/opt/otp/ot_simple_rest/venv/bin/pip3 install -r requirements.txt
	cp -r /opt/otp/ot_simple_rest/venv venv
	#cd ot_simple_rest; python3 -m venv --copies ./python3
	#cd ot_simple_rest; python3/bin/pip3 install -r ../requirements.txt

ot_simple_rest.pth:
	echo Create ot_simple_rest.pth file
	echo "$(BASE_PTH)$(OT_REST_PTH)" >> $@

tests.pth:
	echo Create tests.pth file
	echo "$(BASE_PTH)$(TESTS_PTH)" >> $@

start.sh:
	echo Create start.sh
	echo -e "#!/bin/bash\n\
\n\
cd /opt/otp/ot_simple_rest/logs\n\
source /opt/otp/ot_simple_rest/venv/bin/activate && /opt/otp/ot_simple_rest/venv/bin/python3 /opt/otp/ot_simple_rest/ot_simple_rest.py > stdout.log 2> stderr.log &\n\
" > $@
	chmod +x $@

stop.sh:
	echo Create stop.sh
	echo -e "#!/bin/bash\n\
\n\
kill \`ps ax | grep \"/opt/otp/ot_simple_rest/venv/bin/python3 /opt/otp/ot_simple_rest/ot_simple_rest.py\" | grep -v grep | awk '{print \$$1}'\`\
" > $@
	chmod +x $@

ot_simple_rest.conf:
	echo -e "[general]\n\
level = DEBUG\n\
logs_path = ./\n\
\n\
[db_conf]\n\
host = localhost\n\
database = dispatcher\n\
user = dispatcher\n\
password = password\n\
\n\
[db_conf_eva]\n\
host = localhost\n\
database = eva\n\
user = dispatcher\n\
password = password\n\
\n\
[db_pool_conf]\n\
min_size = 10\n\
max_size = 20\n\
\n\
[mem_conf]\n\
path = /opt/otp/caches/\n\
\n\
[dispatcher]\n\
tracker_max_interval = 60\n\
\n\
[resolver]\n\
no_subsearch_commands = foreach,appendpipe\n\
\n\
[static]\n\
use_nginx = True\n\
base_url = cache/{}\n\
\n\
[user]\n\
check_index_access = False\n\
" > $@

nginx:
	echo Create nginx configs
	mkdir $@
	mkdir $@/conf.d
	cp nginx_example_configs/nginx.conf $@
	cp nginx_example_configs/nginx_eva.conf $@/conf.d

clean_nginx:
	echo Cleaning nginx directory
	rm -rf nginx

clean: .ot_simple_rest.pid
	rm -rf /opt/otp/ot_simple_rest/venv venv build start.sh stop.sh ot_simple_rest.conf nginx ot_simple_rest.tar.gz tests.pth ot_simple_rest.pth /tmp/caches ot_simple_rest-*.tar.gz
	if sudo -u postgres psql -l | grep test_eva > /dev/null; then echo "Drop DB..."; tests/rest/drop_db.sh; fi;

test: venv init_db ot_simple_rest.pid
	echo "Testing..."
	venv/bin/python tests/test_rest.py || (kill -9 `cat ot_simple_rest.pid` && rm -f ot_simple_rest.pid && exit 2)
	kill -9 `cat ot_simple_rest.pid`
	rm -f ot_simple_rest.pid
	rm -rf /tmp/caches

ot_simple_rest.pid:
	echo "Starting daemon for testing"
	venv/bin/python ot_simple_rest/ot_simple_rest.py & echo $$! > ot_simple_rest.pid
	sleep 2

.ot_simple_rest.pid:
	if [ -e ot_simple_rest.pid ]; then kill -9 `cat ot_simple_rest.pid`; rm -f ot_simple_rest.pid; fi

init_db:
	echo "Check or create DB"
	if sudo -u postgres psql -l | grep test_eva; then echo "Drop existing DB..."; tests/rest/drop_db.sh; fi;
	echo "Create DB..."
	tests/rest/create_db.sh

