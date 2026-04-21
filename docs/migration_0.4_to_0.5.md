# Migrating from 0.4.x to 0.5.x

## `register_collection`

Every collection must declare a **top-level primary key** field name:

```python
# 0.4.x
db.register_collection("books", fields_json)

# 0.5.x
db.register_collection("books", fields_json, "title")
```

```rust
// 0.4.x
db.register_collection("books", fields)?;

// 0.5.x
db.register_collection("books", fields, "title")?;
```

Legacy databases opened with **catalog wire v1** (collections **without** a stored primary field) can still be opened for **catalog** operations; **insert** requires a primary key on the collection (re-register or recreate collections with a PK).

## File format

New databases use file format minor **5**. Older files are upgraded lazily: **3 → 4** on first schema write (unchanged from 0.4.x), **4 → 5** on first **record** write.
