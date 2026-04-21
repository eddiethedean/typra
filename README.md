# Typra

> **SQLite simplicity, with real types.**

Typra is a **typed, embedded database** for application data.  
It combines the ease of SQLite with **strict schemas, validation, and nested data support**—so your data is always correct by design.

---

## ✨ Why Typra?

Modern applications already define their data using:
- Rust structs
- Pydantic models
- TypeScript schemas

But most databases ignore that structure and accept loosely typed data.

**Typra fixes that.**

With Typra:
- Your models *are* your database schema
- Invalid data is rejected at write time
- Nested objects and lists are first-class
- Everything lives in a single local file

---

## 🚀 Features

- **🧠 Type-first design**  
  Define your data once using native language types.

- **✅ Validation on write**  
  Bad data never enters your database.

- **🧩 Nested data support**  
  Objects and lists are fully typed and queryable.

- **⚡ Embedded & zero-config**  
  No server. No setup. Just a file.

- **🔄 Safe schema evolution**  
  Migrations are guided and predictable.

- **🔍 Typed queries**  
  Query using field-safe, autocompletable APIs.

---

## 🆚 Typra vs SQLite

| Feature              | SQLite        | Typra                     |
|---------------------|--------------|---------------------------|
| Typing              | Weak         | Strong                    |
| Validation          | Minimal      | Built-in                  |
| Nested data         | JSON hacks   | Native                    |
| Schema evolution    | Manual       | Guided                    |
| API                 | SQL          | Model-first               |
| Setup               | Easy         | Easy                      |

---

## 🐍 Python Example

```python
import typra
from pydantic import BaseModel, EmailStr
from typing import Literal

class Profile(BaseModel):
    display_name: str
    timezone: str

class User(BaseModel):
    id: str
    email: EmailStr
    role: Literal["admin", "member"]
    profile: Profile

db = typra.Database("app.db")
db.register(User)

db.users.insert(User(
    id="1",
    email="user@example.com",
    role="member",
    profile={"display_name": "Odos", "timezone": "UTC"}
))
```

---

## 🦀 Rust Example

```rust
use typra::prelude::*;
use uuid::Uuid;

#[derive(DbModel)]
struct Profile {
    display_name: String,
    timezone: String,
}

#[derive(DbModel)]
struct User {
    #[db(primary)]
    id: Uuid,

    #[db(unique, validate = "email")]
    email: String,

    role: Role,
    profile: Profile,
}

#[derive(DbEnum)]
enum Role {
    Admin,
    Member,
}
```

---

## 📦 Installation

### Python (coming soon)
pip install typra

### Rust (coming soon)
[dependencies]
typra = "0.1"

---

## 🎯 Philosophy

> **Your data should be correct by construction.**

---

## Development

See [docs/contributing.md](docs/contributing.md) for the workspace layout, how to build, and how to publish crates.

Design specs live under [docs/](docs/).

## License

MIT — see [LICENSE](LICENSE).
