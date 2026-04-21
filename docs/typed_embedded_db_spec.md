
# Typed Embedded Database (Working Name) – Full Specification

## Overview
A lightweight, embedded, file-based database written in Rust that provides:
- Strict schemas
- First-class typing
- Nested objects and lists
- Validation at write time
- Safe schema evolution

This database is designed as a modern alternative to SQLite for application-layer data.

---

## Core Principles

1. **Schema-first, not table-first**
2. **Validation is mandatory, not optional**
3. **Nested data is native**
4. **Single-file, zero-config**
5. **Safe evolution over time**

---

## Data Model

### Primitive Types
- bool
- int (8/16/32/64)
- uint (8/16/32/64)
- float (32/64)
- string
- bytes
- uuid
- datetime

### Composite Types
- Optional[T]
- List[T]
- Object
- Enum

---

## Example Schema

```rust
collection User {
    id: Uuid @primary
    email: String @unique @validate(email)
    name: String
    age: Option<U16>
    role: Enum["admin", "member", "viewer"]
    profile: {
        display_name: String,
        timezone: String
    }
    tags: List<String>
    created_at: Timestamp
}
```

---

## Validation Model

### 1. Type Validation
- Enforces strict typing
- Rejects invalid structures

### 2. Field Constraints
- Length, regex, ranges
- Uniqueness

### 3. Record-Level Validation
- Cross-field rules
- Custom validators

---

## Storage Engine

### Architecture
- Append-only log
- Secondary indexes
- Snapshot reads
- Background compaction

### Components
1. Schema Catalog
2. Record Log
3. Index Engine
4. Snapshot Reader
5. Compactor

---

## Write Model

- All writes validated before commit
- Updates internally replace full record
- Append-only storage ensures durability

---

## Query Model

### Example (Python)
```python
db.users.where(
    User.role == "member",
    User.profile.timezone == "UTC"
).limit(10)
```

---

## Indexing

- Primary key indexes
- Unique indexes
- Nested field indexes

Example:
```
index(profile.timezone)
unique(email)
```

---

## Transactions

- ACID compliant
- Single writer, multiple readers
- Snapshot isolation

---

## Schema Evolution

### Safe Changes
- Add optional field
- Add enum value
- Add index

### Breaking Changes
- Remove field
- Narrow types
- Remove enum value

### Migration Example

```rust
Migration {
    from: 1,
    to: 2,
    changes: [
        AddField { name: "age", type: Optional<U8> }
    ]
}
```

---

## Python API

```python
class User(Model):
    id: UUID
    email: EmailStr
    role: Literal["admin", "member"]

db = Database("app.db")
db.register(User)

db.users.insert(User(...))
```

---

## Rust API

```rust
#[derive(DbModel)]
struct User {
    id: Uuid,
    email: String,
    role: Role,
}

db.insert(user)?;
```

---

## Concurrency Model

- Single process
- One writer
- Multiple concurrent readers

---

## Roadmap

### v1
- Core storage engine
- Schema + validation
- Python bindings

### v2
- Migrations
- Advanced queries
- Better indexing

### v3
- Sync / replication
- WASM support
- Optional SQL layer

---

## Key Differentiators

- Strong typing
- Native nested support
- Schema evolution safety
- Developer-first ergonomics

---

## Positioning

"SQLite simplicity with modern typed data guarantees."
