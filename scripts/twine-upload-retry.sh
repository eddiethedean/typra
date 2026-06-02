#!/usr/bin/env bash
# Upload wheels/sdists to PyPI with retries (transient upload.pypi.org timeouts).
set -euo pipefail
if [[ $# -lt 1 ]]; then
  echo "usage: $0 <path>..." >&2
  exit 1
fi
python -m pip install -q --upgrade pip "twine>=5,<6" "pkginfo>=1.12.1.2"
max_attempts=5
delay=30
for ((attempt = 1; attempt <= max_attempts; attempt++)); do
  if python -m twine upload --skip-existing "$@"; then
    exit 0
  fi
  if [[ $attempt -eq max_attempts ]]; then
    echo "twine upload failed after ${max_attempts} attempts" >&2
    exit 1
  fi
  echo "twine upload failed (attempt ${attempt}/${max_attempts}); retrying in ${delay}s..." >&2
  sleep "$delay"
  delay=$((delay * 2))
done
