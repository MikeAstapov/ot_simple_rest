1. ./create_db.sh - creates test_dispatcher & test_eva DBs
2. Start OT_REST instance with ot_simple_rest.conf on port 50001
3. Start tests from project directory /ot_simple_rest by command python tests/test_rest.py
4. After testing shutdown OT_REST instance on port 50001
5. ./drop_db.sh for drop test DBs and user
