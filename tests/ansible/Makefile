VIRTUAL_ENV       ?= $(shell pwd)/.venv
PATH              := $(VIRTUAL_ENV)/bin:$(PATH)
PYTHON_BIN        ?= $(VIRTUAL_ENV)/bin/python
TARGET            ?= none
REQUIREMENTS_FILE ?= requirements.txt
ANSIBLE_CONFIG    ?= $(shell pwd)/ansible.cfg

export PATH

.venv:
	python3 -m venv $(VIRTUAL_ENV)

venv: .venv pip-install

pip-freeze:
	$(PYTHON_BIN) -m pip freeze > $(REQUIREMENTS_FILE)

pip-install:
	$(PYTHON_BIN) -m pip install -r $(REQUIREMENTS_FILE)

server-up:
	ansible-playbook \
		-l "$(TARGET)" \
		--tags up \
		-v playbook.yml

server-down:
	ansible-playbook \
		-l "$(TARGET)" \
		--tags down \
		-v playbook.yml
