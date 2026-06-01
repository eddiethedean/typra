#!/usr/bin/env python3
"""Minimal todo list backed by ModelVault — CRUD, indexes, and queries.

Run from repo root after `make python-develop`:
  .venv/bin/python examples/todo_app/main.py
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

from pydantic import BaseModel

import modelvault

DB_PATH = Path(__file__).resolve().parent / "tasks.modelvault"


class Task(BaseModel):
    __modelvault_primary_key__ = "id"
    __modelvault_indexes__ = [
        modelvault.models.index("done"),
        modelvault.models.unique("id"),
    ]

    id: int
    title: str
    done: bool = False


def open_db() -> tuple[modelvault.Database, object]:
    db = modelvault.Database.open(str(DB_PATH))
    tasks = modelvault.models.collection(db, Task)
    return db, tasks


def cmd_add(tasks: object, title: str) -> None:
    existing = tasks.all()
    next_id = max((t.id for t in existing), default=0) + 1
    tasks.insert(Task(id=next_id, title=title, done=False))
    print(f"added task {next_id}: {title}")


def cmd_list(tasks: object, *, open_only: bool) -> None:
    if open_only:
        rows = tasks.where(Task.done, False).all()
    else:
        rows = tasks.all()
    for t in sorted(rows, key=lambda x: x.id):
        mark = "x" if t.done else " "
        print(f"[{mark}] {t.id}: {t.title}")


def cmd_done(tasks: object, task_id: int) -> None:
    row = tasks.get(task_id)
    if row is None:
        print(f"no task with id {task_id}", file=sys.stderr)
        sys.exit(1)
    tasks.update(task_id, {"done": True})
    print(f"completed task {task_id}")


def cmd_delete(db: modelvault.Database, tasks: object, task_id: int) -> None:
    if tasks.get(task_id) is None:
        print(f"no task with id {task_id}", file=sys.stderr)
        sys.exit(1)
    db.delete(tasks.name, task_id)
    print(f"deleted task {task_id}")


def main() -> None:
    parser = argparse.ArgumentParser(description="ModelVault todo example")
    sub = parser.add_subparsers(dest="command", required=True)

    add_p = sub.add_parser("add", help="add a task")
    add_p.add_argument("title")

    sub.add_parser("list", help="list all tasks")
    sub.add_parser("open", help="list open tasks only")

    done_p = sub.add_parser("done", help="mark task done")
    done_p.add_argument("id", type=int)

    del_p = sub.add_parser("delete", help="delete a task")
    del_p.add_argument("id", type=int)

    args = parser.parse_args()
    db, tasks = open_db()

    if args.command == "add":
        cmd_add(tasks, args.title)
    elif args.command == "list":
        cmd_list(tasks, open_only=False)
    elif args.command == "open":
        cmd_list(tasks, open_only=True)
    elif args.command == "done":
        cmd_done(tasks, args.id)
    elif args.command == "delete":
        cmd_delete(db, tasks, args.id)


if __name__ == "__main__":
    main()
