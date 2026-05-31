#!/usr/bin/env python3
"""CLI-style notes store — local persistence beyond JSON files.

Run from repo root after `make python-develop`:
  .venv/bin/python examples/cli_notes/main.py add "meeting notes"
  .venv/bin/python examples/cli_notes/main.py list
"""
from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import typra

DB_PATH = Path(__file__).resolve().parent / "notes.typra"


@dataclass
class Note:
    __typra_primary_key__ = "id"
    __typra_indexes__ = [typra.models.index("created_at")]

    id: int
    body: str
    created_at: str  # ISO-8601 UTC


def open_notes():
    db = typra.Database.open(str(DB_PATH))
    return db, typra.models.collection(db, Note)


def cmd_add(notes: object, body: str) -> None:
    rows = notes.all()
    next_id = max((n.id for n in rows), default=0) + 1
    ts = datetime.now(timezone.utc).isoformat()
    notes.insert(Note(id=next_id, body=body, created_at=ts))
    print(f"saved note {next_id}")


def cmd_list(notes: object) -> None:
    for n in sorted(notes.all(), key=lambda x: x.id):
        print(f"{n.id} [{n.created_at}] {n.body}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Typra CLI notes example")
    sub = parser.add_subparsers(dest="command", required=True)
    add_p = sub.add_parser("add")
    add_p.add_argument("body")
    sub.add_parser("list")
    args = parser.parse_args()
    _db, notes = open_notes()
    if args.command == "add":
        cmd_add(notes, args.body)
    elif args.command == "list":
        cmd_list(notes)
    else:
        print("unknown command", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
