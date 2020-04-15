
.SILENT:

COMPONENTS := venv start.sh stop.sh ot_simple_rest.conf ot_simple_rest.conf nginx sql

all:
	echo ALL

ot_simple_rest.tar.gz: build
	cd build; tar czf ../ot_simple_rest.tar.gz ot_simple_rest nginx sql && rm -rf ../build

build: $(COMPONENTS)
	echo Build
	mkdir build
	cp -r ot_simple_rest build
	cp -r venv build/ot_simple_rest
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

clean: 
	rm -rf /opt/otp/ot_simple_rest/venv venv build start.sh stop.sh ot_simple_rest.conf nginx ot_simple_rest.tar.gz

test:
	echo "Testing..."
