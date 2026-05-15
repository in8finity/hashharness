.PHONY: install server server-http health

VENV := .venv
PYTHON := $(VENV)/bin/python
HTTP_HOST ?= 127.0.0.1
HTTP_PORT ?= 38417
STORAGE_BACKEND ?= sqlite
DATA_DIR ?= $(HOME)/.hashharness/hashharness.sqlite

$(PYTHON):
	python3 -m venv $(VENV)
	$(PYTHON) -m pip install -e .

install: $(PYTHON)

server: server-http

server-http: $(PYTHON)
	HASHHARNESS_MCP_TRANSPORT=http \
	HASHHARNESS_HTTP_HOST=$(HTTP_HOST) \
	HASHHARNESS_HTTP_PORT=$(HTTP_PORT) \
	HASHHARNESS_STORAGE_BACKEND=$(STORAGE_BACKEND) \
	HASHHARNESS_DATA_DIR="$(DATA_DIR)" \
	$(PYTHON) -m hashharness.mcp_server

health:
	curl -fsS "http://$(HTTP_HOST):$(HTTP_PORT)/health"
