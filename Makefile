.ONESHELL:
SHELL := /bin/bash

.PHONY: run monitor validate v2-data v2-targets v2-model v2-report v2-all

PYTHON := $(CURDIR)/.venv_pr/bin/python
# Official v2 capstone workflow (override if needed: `make v2-all V2PY=python3`)
V2PY ?= $(if $(wildcard $(CURDIR)/.venv-v2/bin/python),$(CURDIR)/.venv-v2/bin/python,python3)
SOFT_LIMIT_GB := 8
HARD_LIMIT_GB := 10
SOFT_LIMIT_KB := 8388608
HARD_LIMIT_KB := 10485760

run:
	if [[ -z "$(strip $(SCRIPT))" ]]; then
		echo "SCRIPT is required. Usage: make run SCRIPT=scripts/run_modeling.py"
		exit 1
	fi
	if [[ ! -x "$(PYTHON)" ]]; then
		echo "Python interpreter not found at $(PYTHON)"
		exit 1
	fi
	if [[ ! -f "$(SCRIPT)" ]]; then
		echo "Script not found: $(SCRIPT)"
		exit 1
	fi
	printf 'Memory limit requested: soft=%sGB (%s KB) hard=%sGB (%s KB)\n' "$(SOFT_LIMIT_GB)" "$(SOFT_LIMIT_KB)" "$(HARD_LIMIT_GB)" "$(HARD_LIMIT_KB)"
	ulimit -H -v $(HARD_LIMIT_KB) || { echo "Failed to set hard virtual memory limit via ulimit."; exit 1; }
	ulimit -S -v $(SOFT_LIMIT_KB) || { echo "Failed to set soft virtual memory limit via ulimit."; exit 1; }
	printf 'Active ulimit -v: soft=%s KB hard=%s KB\n' "$$(ulimit -S -v)" "$$(ulimit -H -v)"
	exec "$(PYTHON)" "$(SCRIPT)"

validate:
	@"$(V2PY)" -m compileall -q "$(CURDIR)/src" "$(CURDIR)/scripts"
	@echo "validate: compileall OK (install pytest locally if you want pytest too)"

monitor:
	target="$(strip $(if $(TARGET),$(TARGET),$(SCRIPT)))"
	if [[ -z "$$target" ]]; then
		echo "TARGET or SCRIPT is required. Usage: make monitor TARGET=scripts/run_modeling.py"
		exit 1
	fi
	if [[ ! -x "$(CURDIR)/scripts/memguard.sh" ]]; then
		echo "Monitor script not found or not executable: $(CURDIR)/scripts/memguard.sh"
		exit 1
	fi
	safe_name="$$(printf '%s' "$$target" | tr -c 'A-Za-z0-9_.-' '_')"
	log_file="/tmp/memguard.$${safe_name}.$$(date +%Y%m%d-%H%M%S).log"
	nohup "$(CURDIR)/scripts/memguard.sh" "$$target" > "$$log_file" 2>&1 &
	printf 'Memory monitor started for %s\nMonitor PID: %s\nLog: %s\n' "$$target" "$$!" "$$log_file"

v2-data:
	"$(V2PY)" "$(CURDIR)/run_pipeline.py"

v2-targets:
	"$(V2PY)" -m src.features.build_targets

v2-model:
	"$(V2PY)" "$(CURDIR)/scripts/run_modeling.py"

v2-report:
	"$(V2PY)" "$(CURDIR)/scripts/generate_model_performance_side_by_side_html.py"

v2-all: v2-data v2-targets v2-model v2-report
	@echo "v2 chain complete. See models/classification_metrics_v2_ordertime.json and docs/html/Model-Performance-SideBySide.html"
