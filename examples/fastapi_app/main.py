#!/usr/bin/env python3
"""Minimal FastAPI + Typra service.

Install extras once:
  .venv/bin/pip install fastapi uvicorn

Run from repo root after `make python-develop`:
  .venv/bin/uvicorn examples.fastapi_app.main:app --reload
"""
from __future__ import annotations

from contextlib import asynccontextmanager
from pathlib import Path

import typra
from fastapi import Depends, FastAPI, HTTPException, Request
from pydantic import BaseModel

DB_PATH = Path(__file__).resolve().parent / "items.typra"


class Item(BaseModel):
    __typra_primary_key__ = "id"
    __typra_indexes__ = [typra.models.index("name")]

    id: int
    name: str
    qty: int


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.db = typra.Database.open(str(DB_PATH))
    app.state.items = typra.models.collection(app.state.db, Item)
    yield


app = FastAPI(title="Typra FastAPI example", lifespan=lifespan)


def get_items(request: Request) -> object:
    return request.app.state.items


@app.post("/items", response_model=Item)
def create_item(body: Item, items=Depends(get_items)):
    existing = items.get(body.id)
    if existing is not None:
        raise HTTPException(status_code=409, detail="id already exists")
    items.insert(body)
    return body


@app.get("/items/{item_id}", response_model=Item)
def read_item(item_id: int, items=Depends(get_items)):
    row = items.get(item_id)
    if row is None:
        raise HTTPException(status_code=404, detail="not found")
    return row


@app.get("/items", response_model=list[Item])
def list_items(items=Depends(get_items)):
    return items.all()


@app.get("/items/search/{name}", response_model=list[Item])
def search_by_name(name: str, items=Depends(get_items)):
    return items.where(Item.name, name).all()
