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

.PHONY: help venv install-tools python-develop test check-full check-python check-rust
.PHONY: coverage coverage-rust coverage-python
.PHONY: ruff-format-check ruff-check ty-check
.PHONY: rust-fmt-check rust-clippy rust-check rust-test

help:
	@echo "Typra Makefile"
	@echo ""
	@echo "Setup:"
	@echo "  venv            Create .venv (if missing)"
	@echo "  install-tools   Install ruff, ty, maturin, pytest into $(PYTHON)"
	@echo "  python-develop  Build/install native extension (maturin develop --release)"
	@echo ""
	@echo "Checks:"
	@echo "  check-full      Python checks + Rust checks + Python tests"
	@echo "  check-python    ruff format/check + ty check (python/)"
	@echo "  check-rust      cargo fmt/clippy/check/test (workspace)"
	@echo ""
	@echo "Tests:"
	@echo "  test            maturin develop --release + pytest (python/typra)"

venv:
	@test -x .venv/bin/python || python3 -m venv .venv
	@$(PYTHON) -m pip -q install -U pip >/dev/null

install-tools: venv
	@$(PYTHON) -m pip -q install -U "ruff>=0.8" "ty>=0.0.28" "maturin>=1.5,<2" "pytest>=8" "pytest-cov>=5" >/dev/null

check-full: check-python check-rust test

check-python: install-tools ruff-format-check ruff-check ty-check

ruff-format-check:
	$(RUFF) format --check python

ruff-check:
	$(RUFF) check python

ty-check:
	$(TY) check python

check-rust: rust-fmt-check rust-clippy rust-check rust-test

rust-fmt-check:
	cargo fmt --all -- --check

rust-clippy:
	cargo clippy --workspace --all-targets --all-features -- -D warnings

rust-check:
	cargo check --workspace --all-targets --all-features

rust-test:
	cargo test --workspace --all-features

python-develop: install-tools
	cd python/typra && env -u VIRTUAL_ENV $(MATURIN) develop --release

test: python-develop
	cd python/typra && env -u VIRTUAL_ENV $(PYTHON) -m pytest -q

coverage: coverage-rust coverage-python

coverage-rust:
	@mkdir -p target/coverage
	@cargo llvm-cov --workspace --all-features \
		--ignore-filename-regex 'python/typra/src/.*' \
		--lcov --output-path target/coverage/rust.lcov
	@cargo llvm-cov --workspace --all-features \
		--ignore-filename-regex 'python/typra/src/.*' \
		--summary-only

coverage-python: python-develop
	@mkdir -p target/coverage
	cd python/typra && env -u VIRTUAL_ENV $(PYTHON) -m pytest -q \
		--cov=tests --cov-report=term-missing \
		--cov-report=xml:../../target/coverage/python.xml

