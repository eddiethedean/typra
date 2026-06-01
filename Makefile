# Quick start (from repo root):
#   python3 -m venv .venv
#   .venv/bin/python -m pip install -U pip
#   make check-full
#
# This Makefile is intentionally modeled after pydantable's "check-full" flow:
# https://github.com/eddiethedean/pydantable/blob/main/Makefile

PYTHON ?= $(CURDIR)/.venv/bin/python
RUFF ?= $(PYTHON) -m ruff
TY ?= $(PYTHON) -m ty
MATURIN ?= $(PYTHON) -m maturin

.PHONY: help venv install-tools python-develop test check-full check-python check-rust verify-doc-examples examples-smoke bench
.PHONY: docs-lint
.PHONY: test-format-compat
.PHONY: check-1p0-ready
.PHONY: docs-install docs-check docs
.PHONY: coverage coverage-rust coverage-python
.PHONY: coverage-rust-core coverage-rust-typra-core
.PHONY: ruff-format-check ruff-check ty-check
.PHONY: rust-fmt-check rust-clippy rust-check rust-doc rust-test

help:
	@echo "Typra Makefile"
	@echo ""
	@echo "Setup:"
	@echo "  venv            Create .venv (if missing)"
	@echo "  install-tools   Install ruff, ty, maturin, pytest into $(PYTHON)"
	@echo "  python-develop  Build/install native extension (maturin develop --release)"
	@echo ""
	@echo "Checks:"
	@echo "  check-full      Python + Rust checks, tests, doc examples, examples-smoke, docs"
	@echo "  check-1p0-ready check-full + test-format-compat + async facade tests"
	@echo "  check-python    ruff format/check + ty check (python/)"
	@echo "  check-rust      cargo fmt/clippy/check/doc/test (workspace)"
	@echo ""
	@echo "Tests:"
	@echo "  test-format-compat  1.x must read 1.0-shaped .typra fixtures"
	@echo "  test            maturin develop --release + pytest (python/typra)"
	@echo "  verify-doc-examples  Assert README + guides output matches all verified Python/Rust snippets"
	@echo "  examples-smoke    Run todo_app + cli_notes example CLIs (requires python-develop)"
	@echo "  bench           Criterion benchmarks for typra-core (optional; not part of check-full)"

venv:
	@test -x .venv/bin/python || python3 -m venv .venv
	@$(PYTHON) -m pip -q install -U pip >/dev/null

install-tools: venv
	@$(PYTHON) -m pip -q install -U "ruff>=0.8" "ty>=0.0.28" "maturin>=1.5,<2" "pytest>=8" "pytest-cov>=5" "pydantic>=2" >/dev/null

check-full: check-python check-rust test verify-doc-examples examples-smoke docs-lint docs-check

# “1.0 readiness” suite (no version bump): contracts + docs + API surfaces.
# - Runs the full cross-language check pipeline.
# - Adds an explicit async-surface compile/test run for the Rust facade.
test-format-compat:
	cargo test -p typra-core --test format_back_compat_1x --all-features

check-1p0-ready: check-full test-format-compat
	cargo test -p typra --features async

check-python: install-tools ruff-format-check ruff-check ty-check

ruff-format-check:
	$(RUFF) format --check python

ruff-check:
	$(RUFF) check python

ty-check: python-develop
	env -u VIRTUAL_ENV $(TY) check --python $(PYTHON) --python-version 3.12 python

check-rust: rust-fmt-check rust-clippy rust-check rust-doc rust-test

rust-fmt-check:
	cargo fmt --all -- --check

rust-clippy:
	cargo clippy --workspace --all-targets --all-features -- -D warnings

rust-check:
	cargo check --workspace --all-targets --all-features

rust-doc:
	env RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --all-features

rust-test:
	cargo test --workspace --all-features

bench:
	cargo bench -p typra-core --bench query

python-develop: install-tools
	cd python/typra && env -u VIRTUAL_ENV $(MATURIN) develop --release

test: python-develop
	cd python/typra && env -u VIRTUAL_ENV $(PYTHON) -m pytest -q

verify-doc-examples: python-develop
	bash ./scripts/verify-doc-examples.sh

docs-lint:
	bash ./scripts/docs-lint.sh

docs-install: venv
	@$(PYTHON) -m pip -q install -r docs/requirements.txt >/dev/null

docs-check: docs-install
	@NO_MKDOCS_2_WARNING=1 $(PYTHON) -m mkdocs build --strict 2>&1 | tee /tmp/typra-mkdocs-build.log; \
	status=$$?; \
	if [ $$status -ne 0 ]; then exit $$status; fi; \
	if grep -qE 'WARNING|excluded from the built site' /tmp/typra-mkdocs-build.log; then \
	  echo "mkdocs build produced warnings (see above)" >&2; \
	  exit 1; \
	fi

docs: docs-check

coverage: coverage-rust coverage-python

# Minimum line coverage for `typra-core` (CI gate via `make coverage-rust`).
COVERAGE_TYPRA_CORE_LINES ?= 90

# Per-module line coverage for db / query / index / validation (`make coverage-rust-core`).
COVERAGE_MODULE_MIN_LINES ?= 90
COVERAGE_CORE_DB_LINES ?= $(COVERAGE_MODULE_MIN_LINES)
COVERAGE_CORE_QUERY_LINES ?= $(COVERAGE_MODULE_MIN_LINES)
COVERAGE_CORE_INDEX_LINES ?= $(COVERAGE_MODULE_MIN_LINES)
COVERAGE_CORE_VALIDATION_LINES ?= $(COVERAGE_MODULE_MIN_LINES)

examples-smoke: python-develop
	@rm -f examples/todo_app/tasks.typra examples/cli_notes/notes.typra
	@rm -rf examples/desktop_app/.smoke-data
	$(PYTHON) examples/todo_app/main.py add "docs smoke"
	$(PYTHON) examples/todo_app/main.py list | grep -q "docs smoke"
	$(PYTHON) examples/todo_app/main.py done 1
	$(PYTHON) examples/todo_app/main.py open
	$(PYTHON) examples/cli_notes/main.py add "cli smoke"
	$(PYTHON) examples/cli_notes/main.py list | grep -q "cli smoke"
	@TYPRA_EXAMPLE_DATA_DIR="$(CURDIR)/examples/desktop_app/.smoke-data" \
		$(PYTHON) examples/desktop_app/main.py | grep -q "initialized theme=dark"
	@TYPRA_EXAMPLE_DATA_DIR="$(CURDIR)/examples/desktop_app/.smoke-data" \
		$(PYTHON) examples/desktop_app/main.py | grep -q "loaded theme="
	@echo "examples-smoke: OK (todo, cli, desktop)"

coverage-rust-typra-core:
	@mkdir -p target/coverage
	@CI=1 cargo llvm-cov -p typra-core --all-features \
		--lcov --output-path target/coverage/typra-core.lcov \
		--fail-under-lines $(COVERAGE_TYPRA_CORE_LINES) --summary-only
	@$(PYTHON) scripts/coverage_core.py target/coverage/typra-core.lcov \
		--db-min-lines $(COVERAGE_CORE_DB_LINES) \
		--query-min-lines $(COVERAGE_CORE_QUERY_LINES) \
		--index-min-lines $(COVERAGE_CORE_INDEX_LINES) \
		--validation-min-lines $(COVERAGE_CORE_VALIDATION_LINES)

coverage-rust: coverage-rust-typra-core
	@CI=1 cargo llvm-cov --workspace --all-features \
		--ignore-filename-regex 'python/typra/src/.*' \
		--lcov --output-path target/coverage/rust.lcov

coverage-rust-core: coverage-rust-typra-core

coverage-python: python-develop
	@mkdir -p target/coverage
	cd python/typra && env -u VIRTUAL_ENV $(PYTHON) -m pytest -q \
		--cov=tests --cov-report=term-missing \
		--cov-report=xml:../../target/coverage/python.xml \
		--cov-fail-under 70

