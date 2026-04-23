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

.PHONY: help venv install-tools python-develop test check-full check-python check-rust verify-doc-examples bench
.PHONY: coverage coverage-rust coverage-python
.PHONY: coverage-rust-core
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
	@echo "  check-full      Python checks + Rust checks + Python tests + doc example outputs"
	@echo "  check-python    ruff format/check + ty check (python/)"
	@echo "  check-rust      cargo fmt/clippy/check/doc/test (workspace)"
	@echo ""
	@echo "Tests:"
	@echo "  test            maturin develop --release + pytest (python/typra)"
	@echo "  verify-doc-examples  Assert README + guides output matches all verified Python/Rust snippets"
	@echo "  bench           Criterion benchmarks for typra-core (optional; not part of check-full)"

venv:
	@test -x .venv/bin/python || python3 -m venv .venv
	@$(PYTHON) -m pip -q install -U pip >/dev/null

install-tools: venv
	@$(PYTHON) -m pip -q install -U "ruff>=0.8" "ty>=0.0.28" "maturin>=1.5,<2" "pytest>=8" "pytest-cov>=5" >/dev/null

check-full: check-python check-rust test verify-doc-examples

check-python: install-tools ruff-format-check ruff-check ty-check

ruff-format-check:
	$(RUFF) format --check python

ruff-check:
	$(RUFF) check python

ty-check:
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

coverage: coverage-rust coverage-python

# Minimum line coverage for `typra-core` (practical gate; raise as tests improve).
COVERAGE_TYPRA_CORE_LINES ?= 92

# "Core logic" coverage gates.
# We compute this from the LCOV output and exclude format/corruption/error-injection-heavy modules.
# Current realistic baselines (raise over time).
COVERAGE_CORE_DB_LINES ?= 84
COVERAGE_CORE_QUERY_LINES ?= 94
COVERAGE_CORE_INDEX_LINES ?= 94
COVERAGE_CORE_VALIDATION_LINES ?= 91

coverage-rust:
	@mkdir -p target/coverage
	@CI=1 cargo llvm-cov --workspace --all-features \
		--ignore-filename-regex 'python/typra/src/.*' \
		--lcov --output-path target/coverage/rust.lcov
	@CI=1 cargo llvm-cov -p typra-core --all-features \
		--fail-under-lines $(COVERAGE_TYPRA_CORE_LINES) --summary-only

coverage-rust-core:
	@mkdir -p target/coverage
	@CI=1 cargo llvm-cov -p typra-core --all-features \
		--lcov --output-path target/coverage/typra-core.lcov
	@$(PYTHON) scripts/coverage_core.py target/coverage/typra-core.lcov \
		--db-min-lines $(COVERAGE_CORE_DB_LINES) \
		--query-min-lines $(COVERAGE_CORE_QUERY_LINES) \
		--index-min-lines $(COVERAGE_CORE_INDEX_LINES) \
		--validation-min-lines $(COVERAGE_CORE_VALIDATION_LINES)

coverage-python: python-develop
	@mkdir -p target/coverage
	cd python/typra && env -u VIRTUAL_ENV $(PYTHON) -m pytest -q \
		--cov=tests --cov-report=term-missing \
		--cov-report=xml:../../target/coverage/python.xml

