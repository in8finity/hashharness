.PHONY: server install

VENV := .venv
PYTHON := $(VENV)/bin/python

$(PYTHON):
	python3 -m venv $(VENV)
	$(PYTHON) -m pip install -e .

install: $(PYTHON)

server: $(PYTHON)
	HASHHARNESS_MCP_TRANSPORT=http \
	HASHHARNESS_HTTP_PORT=38417 \
	HASHHARNESS_STORAGE_BACKEND=sqlite \
	HASHHARNESS_DATA_DIR="$$HOME/.hashharness/hashharness.sqlite" \
	$(PYTHON) -m hashharness.mcp_server
