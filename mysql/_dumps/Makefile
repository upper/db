TEST_HOST   ?= 127.0.0.1
TEST_PORT   ?= 3306

DB_USERNAME ?= upperio_tests
DB_PASSWORD ?= upperio_secret
DB_NAME     ?= upperio_tests

load:
	cat structs.sql | mysql -u"$(DB_USERNAME)" -p"$(DB_PASSWORD)" "$(DB_NAME)" -h"$(TEST_HOST)" -p$(TEST_PORT)
