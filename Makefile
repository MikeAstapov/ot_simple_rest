
#.SILENT:

COMPONENTS := start.sh stop.sh nginx sql dist ot_simple_rest.conf

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

pack: make_build
	$(SET_VERSION)
	$(SET_BRANCH)
	rm -f ot_simple_rest-*.tar.gz
	echo Create archive \"ot_simple_rest-$(VERSION)-$(BRANCH).tar.gz\"
	cd make_build; tar czf ../ot_simple_rest-$(VERSION)-$(BRANCH).tar.gz ot_simple_rest nginx sql

clean_pack:
	rm -f ot_simple_rest-*.tar.gz


ot_simple_rest.tar.gz: build
	cd build; tar czf ../ot_simple_rest.tar.gz ot_simple_rest nginx sql && rm -rf ../make_build

build: make_build

make_build: $(COMPONENTS)
	# required section
	echo make_build
	mkdir make_build
	mkdir make_build/ot_simple_rest/
	ln -s /opt/otp/logs/ot_simple_rest make_build/ot_simple_rest/logs
	cp start.sh make_build/ot_simple_rest/start.sh
	cp stop.sh make_build/ot_simple_rest/stop.sh
	cp ot_simple_rest/ot_simple_rest.conf.example make_build/ot_simple_rest/ot_simple_rest.conf.example
	cp ot_simple_rest.conf make_build/ot_simple_rest/ot_simple_rest.conf
	cp -r nginx make_build
	cp -r sql make_build
	cp ./dist/ot_simple_rest make_build/ot_simple_rest/
	cp *.md make_build/ot_simple_rest/
	cp -r docs/macros make_build/ot_simple_rest/

clean_build:
	rm -rf make_build
	rm -f ot_simple_rest.conf
	rm -rf start.sh
	rm -rf stop.sh


dist: venv
	./venv/bin/pyinstaller --runtime-tmpdir ./tmp --hidden-import=_cffi_backend -F ot_simple_rest/ot_simple_rest.py

clean_dist:
	rm -rf build
	rm -rf dist
	rm -f ot_simple_rest.spec

venv:
	echo Create venv
	python3 -m venv ./venv
	./venv/bin/pip3 install -r requirements.txt

clean_venv:
	rm -rf venv

start.sh:
	echo Create start.sh
	echo -e "#!/bin/bash\n\
\n\
cd /opt/otp/ot_simple_rest/\n\
mkdir -p tmp\n\
rm -rf tmp/*\n\
/opt/otp/ot_simple_rest/ot_simple_rest > logs/stdout.log 2> logs/stderr.log &\
" > $@
	chmod +x $@

stop.sh:
	echo Create stop.sh
	echo -e "#!/bin/bash\n\
\n\
kill \`ps ax | grep \"ot_simple_rest\" | grep -v grep | awk '{print \$$1}'\`\
" > $@
	chmod +x $@

ot_simple_rest.conf:
	echo -e "[general]\n\
level = DEBUG\n\
logs_path = ./logs/\n\
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
macros_dir = /opt/otp/ot_simple_rest/macros/\n\
\n\
[static]\n\
use_nginx = True\n\
base_url = cache/{}\n\
static_path = /opt/otp/static/\n\
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

clean: .ot_simple_rest.pid clean_build clean_dist clean_venv clean_nginx clean_pack clean_test
	#rm -rf /opt/otp/ot_simple_rest/venv venv build start.sh stop.sh ot_simple_rest.conf nginx ot_simple_rest.tar.gz ot_simple_rest.pth /tmp/caches ot_simple_rest-*.tar.gz
	#if sudo -u postgres psql -l | grep test_eva > /dev/null; then echo "Drop DB..."; tests/rest/drop_db.sh; fi;

test: venv init_db ot_simple_rest.pid
	echo "Testing..."
	export PYTHONPATH=./ot_simple_rest/:./tests/; ./venv/bin/python -m unittest || (kill -9 `cat ot_simple_rest.pid` && rm -f ot_simple_rest.pid && exit 2)
	kill -9 `cat ot_simple_rest.pid`
	rm -f ot_simple_rest.pid
	rm -rf /tmp/caches
	rm -f ot_simple_rest/ot_simple_rest.conf

clean_test:
	rm -f ot_simple_rest.pid
	rm -rf /tmp/caches
	rm -f ot_simple_rest/ot_simple_rest.conf


ot_simple_rest.pid: venv
	echo "Starting daemon for testing"
	cp ot_simple_rest/ot_simple_rest.conf.example ot_simple_rest/ot_simple_rest.conf
	venv/bin/python ot_simple_rest/ot_simple_rest.py & echo $$! > ot_simple_rest.pid
	sleep 2

.ot_simple_rest.pid:
	if [ -e ot_simple_rest.pid ]; then kill -9 `cat ot_simple_rest.pid`; rm -f ot_simple_rest.pid; fi

init_db:
	echo "Check or create DB"
	if sudo -u postgres psql -l | grep test_eva; then echo "Drop existing DB..."; tests/rest/drop_db.sh; fi;
	echo "Create DB..."
	tests/rest/create_db.sh

