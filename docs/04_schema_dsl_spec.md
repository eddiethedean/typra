# Typed Embedded Database – Schema DSL Specification

## Goals
The schema DSL should be:
- human-readable
- machine-parseable
- versionable
- expressive enough for strict typed records
- friendly to code generation
- suitable for migrations and tooling

There may eventually be multiple schema representations:
1. Rust derive macros
2. Python model inference
3. TypeScript schema builders
4. text DSL

This document defines the **text DSL** and its conceptual schema model.

## Design Principles
1. Collections are the top-level persistent unit.
2. Schemas are strict by default.
3. Types are explicit.
4. Defaults and validators are attached declaratively.
5. Nested objects are first-class.
6. Schema evolution compares contracts, not just field lists.

## Example

```tdb
collection User {
  id: uuid @primary
  email: string @unique @validate(email)
  name: string
  age: optional<uint16>
  role: enum["admin", "member", "viewer"]
  profile: object {
    display_name: string
    timezone: string
    marketing_opt_in: bool = false
  }
  tags: list<string> = []
  created_at: timestamp
}
```

## Grammar Sketch

```text
schema          := collection_def*
collection_def  := "collection" IDENT "{" field_def* "}"
field_def       := IDENT ":" type_expr field_mod*
field_mod       := default_mod | attribute_mod
default_mod     := "=" literal
attribute_mod   := "@" IDENT ["(" arg_list ")"]
type_expr       := primitive
                | "optional" "<" type_expr ">"
                | "list" "<" type_expr ">"
                | enum_expr
                | object_expr
enum_expr       := "enum" "[" literal ("," literal)* "]"
object_expr     := "object" "{" field_def* "}"
```

## Type System

### Primitive Types
- `bool`
- `int8`, `int16`, `int32`, `int64`
- `uint8`, `uint16`, `uint32`, `uint64`
- `float32`, `float64`
- `string`
- `bytes`
- `uuid`
- `date`
- `time`
- `timestamp`

### Composite Types
- `optional<T>`
- `list<T>`
- `enum[...]`
- `object { ... }`

## Field Attributes

### Identity / Keys
- `@primary`
- `@unique`
- `@index`

### Validation
- `@validate(email)`
- `@validate(url)`
- `@validate(regex="^[A-Z]+$")`

### Constraint Attributes
- `@min(0)`
- `@max(100)`
- `@length(min=3,max=100)`
- `@nonempty`

### Migration / Compatibility Hints
- `@since(3)`
- `@deprecated`
- `@rename_from("old_name")`

### Storage Hints
- `@compact`
- `@doc("human description")`

## Defaults
Defaults may be:
- scalar literals
- `null`
- empty list `[]`
- empty object `{}` if supported
- engine constants later, e.g. `@default(now())`

For v1, prefer static defaults.
Dynamic defaults such as `now()` can be added later as engine-generated values.

## Nullability and Optionality
Text DSL should make optionality explicit.

### Examples
```tdb
name: string
nickname: optional<string>
tags: list<string> = []
```

Meaning:
- `name`: required, non-null
- `nickname`: nullable/optional by contract
- `tags`: if omitted, default empty list

If the engine later distinguishes omitted vs null more strictly, represent that in schema metadata rather than overcomplicating text syntax initially.

## Nested Objects
Nested object fields are declared inline:

```tdb
profile: object {
  display_name: string
  timezone: string
}
```

This must compile into path-addressable field metadata:
- `profile.display_name`
- `profile.timezone`

## Lists
Lists are homogeneous:

```tdb
tags: list<string>
line_items: list<object {
  sku: string
  qty: uint16
}>
```

Each element type is validated recursively.

## Enums
Enums are explicit finite sets:

```tdb
role: enum["admin", "member", "viewer"]
```

Rules:
- literals are strings for v1
- order preserved in metadata
- adding members may be safe
- removing members is breaking

## Collection-Level Attributes
Possible future syntax:
```tdb
collection User @strict @history {
  ...
}
```

Potential collection attributes:
- `@strict`
- `@history`
- `@doc("...")`

## Schema Metadata Model
The DSL compiles into structured metadata roughly like:

```json
{
  "collection": "User",
  "version": 1,
  "fields": [
    {"path": "id", "type": "uuid", "primary": true},
    {"path": "email", "type": "string", "unique": true, "validators": ["email"]},
    {"path": "role", "type": "enum", "members": ["admin", "member", "viewer"]}
  ]
}
```

## Validation Semantics
Validation rules derive from:
1. type expression
2. default presence
3. attributes
4. custom validators

### Example
```tdb
age: uint8 @min(13) @max(120)
username: string @length(min=3,max=32) @validate(regex="^[a-z0-9_]+$")
```

## Migration Semantics in DSL
Schema diffs use field paths and type metadata.
Helpful hints:
- `@rename_from("old_name")`
- `@deprecated`

Example:
```tdb
display_name: string @rename_from("name")
```

This does not itself perform schema evolution, but informs planning.

## Alternate Representation: Rust
Rust derive should map to equivalent schema metadata.

```rust
#[derive(DbModel)]
struct User {
    #[db(primary)]
    id: Uuid,
    #[db(unique, validate = "email")]
    email: String,
    role: Role,
}
```

## Alternate Representation: Python
Python/Pydantic inference can generate same metadata.

```python
class User(BaseModel):
    id: UUID
    email: EmailStr
    role: Literal["admin", "member", "viewer"]
```

## Alternate Representation: TypeScript
```ts
const User = t.collection("User", {
  id: t.uuid().primary(),
  email: t.string().email().unique(),
  role: t.enum(["admin", "member", "viewer"]),
})
```

## Parser Considerations
The text DSL parser should:
- preserve source locations for errors
- support comments
- support trailing commas
- produce AST then normalized schema metadata

### Comments
```tdb
// line comment
/* block comment */
```

## Error Reporting
Parser and compiler errors should include:
- file name
- line and column
- nearby snippet
- hint

Example:
> Unknown type `sting`; did you mean `string`?

## Canonicalization
Normalize schemas before comparison:
- order field metadata deterministically
- preserve semantic meaning
- compute stable schema hash

## Minimal v1 Scope
Support:
- collections
- fields
- primitives
- optional
- list
- enum
- nested object
- defaults
- primary/unique/index
- basic validators and bounds

## Future Extensions
- references
- tagged unions
- computed fields
- generated fields
- foreign-key-like semantics
- reusable named object types
- import/include syntax
