//! Python model helpers: class-based schemas and typed-ish handles.

use std::borrow::Cow;

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyAnyMethods, PyDict, PyList, PyString, PyTuple};

use typra_core::schema::{FieldPath, Type};
use typra_core::FieldDef;

use crate::database::{lock_inner, Database};
use crate::errors::db_error_to_py;

#[pyclass(from_py_object)]
#[derive(Clone)]
pub struct IndexSpec {
    #[pyo3(get)]
    pub name: Option<String>,
    #[pyo3(get)]
    pub field: String,
    #[pyo3(get)]
    pub unique: bool,
}

#[pyclass]
pub struct ConstraintSpec {
    #[pyo3(get)]
    pub kind: String,
    #[pyo3(get)]
    pub value: Option<Py<PyAny>>,
}

#[pyclass(from_py_object)]
#[derive(Clone)]
pub struct FieldRef {
    #[pyo3(get)]
    pub path: Vec<String>,
}

#[pyfunction]
#[pyo3(signature = (field, name=None))]
pub fn index(field: &str, name: Option<String>) -> PyResult<IndexSpec> {
    Ok(IndexSpec {
        name,
        field: field.to_string(),
        unique: false,
    })
}

#[pyfunction]
#[pyo3(signature = (field, name=None))]
pub fn unique(field: &str, name: Option<String>) -> PyResult<IndexSpec> {
    Ok(IndexSpec {
        name,
        field: field.to_string(),
        unique: true,
    })
}

/// Create a constraint spec for `typing.Annotated` metadata.
///
/// Example:
///     year: Annotated[int, typra.models.constrained(min_i64=0)]
#[pyfunction]
#[pyo3(signature = (**kwargs))]
pub fn constrained(
    _py: Python<'_>,
    kwargs: Option<&Bound<'_, PyDict>>,
) -> PyResult<Vec<ConstraintSpec>> {
    let mut out = Vec::new();
    let Some(kwargs) = kwargs else {
        return Ok(out);
    };
    for (k, v) in kwargs.iter() {
        let k: String = k.extract()?;
        out.push(ConstraintSpec {
            kind: k,
            value: if v.is_none() { None } else { Some(v.unbind()) },
        });
    }
    Ok(out)
}

fn ensure_field_refs(py: Python<'_>, cls: &Bound<'_, PyAny>) -> PyResult<()> {
    let hints = get_type_hints(py, cls)?;
    let hints = hints.bind(py);
    for (k, _) in hints.iter() {
        let name: String = k.extract()?;
        // Don't overwrite user-defined attributes.
        if cls.getattr(name.as_str()).is_ok() {
            continue;
        }
        let r = FieldRef {
            path: vec![name.clone()],
        };
        let obj = Py::new(py, r)?;
        cls.setattr(name.as_str(), obj)?;
    }
    Ok(())
}

fn snake_case(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for (i, ch) in s.chars().enumerate() {
        if ch.is_ascii_uppercase() {
            if i != 0 {
                out.push('_');
            }
            out.push(ch.to_ascii_lowercase());
        } else {
            out.push(ch);
        }
    }
    out
}

fn pluralize(s: &str) -> String {
    if s.ends_with('s') {
        format!("{s}es")
    } else {
        format!("{s}s")
    }
}

fn collection_name_for(_py: Python<'_>, cls: &Bound<'_, PyAny>) -> PyResult<String> {
    if let Ok(v) = cls.getattr("__typra_collection__") {
        if !v.is_none() {
            return v.extract::<String>();
        }
    }
    let name: String = cls.getattr("__name__")?.extract()?;
    Ok(pluralize(&snake_case(&name)))
}

fn primary_key_for(cls: &Bound<'_, PyAny>) -> PyResult<String> {
    let v = cls.getattr("__typra_primary_key__").map_err(|_| {
        PyValueError::new_err("model must define __typra_primary_key__ = \"field\"")
    })?;
    let pk: String = v
        .extract()
        .map_err(|_| PyValueError::new_err("__typra_primary_key__ must be a string field name"))?;
    if pk.trim().is_empty() {
        return Err(PyValueError::new_err(
            "__typra_primary_key__ cannot be empty",
        ));
    }
    Ok(pk)
}

fn typing_mod(py: Python<'_>) -> PyResult<Bound<'_, PyModule>> {
    PyModule::import(py, "typing")
}

fn dataclasses_mod(py: Python<'_>) -> PyResult<Bound<'_, PyModule>> {
    PyModule::import(py, "dataclasses")
}

fn is_dataclass(py: Python<'_>, cls: &Bound<'_, PyAny>) -> PyResult<bool> {
    let dc = dataclasses_mod(py)?;
    let f = dc.getattr("is_dataclass")?;
    f.call1((cls,))?.extract()
}

fn pydantic_is_model(py: Python<'_>, cls: &Bound<'_, PyAny>) -> PyResult<bool> {
    let Ok(pyd) = PyModule::import(py, "pydantic") else {
        return Ok(false);
    };
    let Ok(base) = pyd.getattr("BaseModel") else {
        return Ok(false);
    };
    let builtins = PyModule::import(py, "builtins")?;
    builtins
        .getattr("issubclass")?
        .call1((cls, base))?
        .extract()
}

fn get_type_hints(py: Python<'_>, cls: &Bound<'_, PyAny>) -> PyResult<Py<PyDict>> {
    let t = typing_mod(py)?;
    let f = t.getattr("get_type_hints")?;
    // Python 3.11+: include_extras preserves typing.Annotated metadata.
    let kwargs = PyDict::new(py);
    let _ = kwargs.set_item("include_extras", true);
    let res = f.call((cls,), Some(&kwargs)).or_else(|_| f.call1((cls,)))?;
    Ok(res.cast_into::<PyDict>()?.unbind())
}

#[allow(clippy::type_complexity)]
fn origin_and_args<'py>(
    py: Python<'py>,
    t: &Bound<'py, PyAny>,
) -> PyResult<(Option<Bound<'py, PyAny>>, Vec<Bound<'py, PyAny>>)> {
    let typing = typing_mod(py)?;
    let origin = typing.getattr("get_origin")?.call1((t,))?;
    let origin = if origin.is_none() { None } else { Some(origin) };
    let args_any = typing.getattr("get_args")?.call1((t,))?;
    let args = if args_any.is_none() {
        Vec::new()
    } else if let Ok(tup) = args_any.cast::<PyTuple>() {
        tup.iter().map(|a| a.to_owned()).collect()
    } else {
        Vec::new()
    };
    Ok((origin, args))
}

fn is_none_type(py: Python<'_>, t: &Bound<'_, PyAny>) -> PyResult<bool> {
    // `types.NoneType` exists on Python 3.10+, but Typra supports Python 3.9+.
    // Use `type(None)` via the runtime singleton instead.
    let builtins = PyModule::import(py, "builtins")?;
    let none_obj = py.None();
    let none_type = builtins.getattr("type")?.call1((none_obj,))?;
    Ok(t.is(&none_type))
}

fn py_to_typra_type(py: Python<'_>, t: &Bound<'_, PyAny>, depth: usize) -> PyResult<Type> {
    if depth > 32 {
        return Err(PyValueError::new_err("model type nesting too deep"));
    }

    let builtins = PyModule::import(py, "builtins")?;
    let str_t = builtins.getattr("str")?;
    let int_t = builtins.getattr("int")?;
    let float_t = builtins.getattr("float")?;
    let bool_t = builtins.getattr("bool")?;
    let bytes_t = builtins.getattr("bytes")?;

    if t.is(&str_t) {
        return Ok(Type::String);
    }
    if t.is(&int_t) {
        return Ok(Type::Int64);
    }
    if t.is(&float_t) {
        return Ok(Type::Float64);
    }
    if t.is(&bool_t) {
        return Ok(Type::Bool);
    }
    if t.is(&bytes_t) {
        return Ok(Type::Bytes);
    }

    // uuid.UUID
    if let Ok(uuid_mod) = PyModule::import(py, "uuid") {
        if let Ok(uuid_t) = uuid_mod.getattr("UUID") {
            if t.is(&uuid_t) {
                return Ok(Type::Uuid);
            }
        }
    }
    // datetime.datetime
    if let Ok(dt_mod) = PyModule::import(py, "datetime") {
        if let Ok(dt_t) = dt_mod.getattr("datetime") {
            if t.is(&dt_t) {
                return Ok(Type::Timestamp);
            }
        }
    }

    let (origin, args) = origin_and_args(py, t)?;

    // Annotated[T, ...]
    if let Some(o) = origin.as_ref() {
        let typing = typing_mod(py)?;
        if o.is(&typing.getattr("Annotated")?) {
            if args.is_empty() {
                return Err(PyValueError::new_err("Annotated must have a base type"));
            }
            return py_to_typra_type(py, &args[0], depth + 1);
        }
    }

    // Optional[T] → Union[T, None]
    if let Some(o) = origin {
        // list[T]
        if let Ok(list_t) = builtins.getattr("list") {
            if o.is(&list_t) {
                if args.len() != 1 {
                    return Err(PyValueError::new_err("List must have one type argument"));
                }
                let inner = py_to_typra_type(py, &args[0], depth + 1)?;
                return Ok(Type::List(Box::new(inner)));
            }
        }

        // Union / Optional
        let typing = typing_mod(py)?;
        let union = typing.getattr("Union")?;
        if o.is(&union) {
            let mut non_none: Vec<Bound<'_, PyAny>> = Vec::new();
            for a in args {
                if !is_none_type(py, &a)? {
                    non_none.push(a);
                }
            }
            if non_none.len() == 1 {
                let inner = py_to_typra_type(py, &non_none[0], depth + 1)?;
                return Ok(Type::Optional(Box::new(inner)));
            }
        }
    }

    // Nested dataclass / pydantic model → object
    if is_dataclass(py, t)? || pydantic_is_model(py, t)? {
        let nested = fields_from_model(py, t, depth + 1)?;
        return Ok(Type::Object(nested));
    }

    // enum.Enum -> string-backed enum variants
    if let Ok(enum_mod) = PyModule::import(py, "enum") {
        if let Ok(enum_base) = enum_mod.getattr("Enum") {
            let builtins = PyModule::import(py, "builtins")?;
            let is_sub: bool = builtins
                .getattr("issubclass")?
                .call1((t, enum_base))?
                .extract()
                .unwrap_or(false);
            if is_sub {
                let members_any = t.getattr("__members__")?;
                let members = members_any.cast::<PyDict>()?;
                let mut variants = Vec::with_capacity(members.len());
                for (k, _) in members.iter() {
                    variants.push(k.extract::<String>()?);
                }
                return Ok(Type::Enum(variants));
            }
        }
    }

    Err(PyValueError::new_err(format!(
        "unsupported model field type: {t:?}"
    )))
}

fn field_constraints_from_annotated(
    py: Python<'_>,
    t: &Bound<'_, PyAny>,
) -> PyResult<Vec<typra_core::schema::Constraint>> {
    let (origin, args) = origin_and_args(py, t)?;
    let Some(origin) = origin else {
        return Ok(Vec::new());
    };
    let typing = typing_mod(py)?;
    if !origin.is(&typing.getattr("Annotated")?) {
        return Ok(Vec::new());
    }
    if args.len() < 2 {
        return Ok(Vec::new());
    }
    // args[1:] are metadata; we support ConstraintSpec or list[ConstraintSpec] returned by constrained().
    let mut out = Vec::new();
    for meta in args.into_iter().skip(1) {
        if let Ok(spec) = meta.extract::<PyRef<'_, ConstraintSpec>>() {
            out.extend(constraint_spec_to_engine(py, &spec)?);
            continue;
        }
        if let Ok(list) = meta.cast::<PyList>() {
            for item in list.iter() {
                if let Ok(spec) = item.extract::<PyRef<'_, ConstraintSpec>>() {
                    out.extend(constraint_spec_to_engine(py, &spec)?);
                }
            }
        }
    }
    Ok(out)
}

fn constraint_spec_to_engine(
    py: Python<'_>,
    spec: &ConstraintSpec,
) -> PyResult<Vec<typra_core::schema::Constraint>> {
    use typra_core::schema::Constraint;
    let k = spec.kind.as_str();
    let v = spec.value.as_ref().map(|x| x.bind(py));
    let one = match k {
        "min_i64" => Constraint::MinI64(
            v.ok_or_else(|| PyValueError::new_err("min_i64 requires a value"))?
                .extract()?,
        ),
        "max_i64" => Constraint::MaxI64(
            v.ok_or_else(|| PyValueError::new_err("max_i64 requires a value"))?
                .extract()?,
        ),
        "min_u64" => Constraint::MinU64(
            v.ok_or_else(|| PyValueError::new_err("min_u64 requires a value"))?
                .extract()?,
        ),
        "max_u64" => Constraint::MaxU64(
            v.ok_or_else(|| PyValueError::new_err("max_u64 requires a value"))?
                .extract()?,
        ),
        "min_f64" => Constraint::MinF64(
            v.ok_or_else(|| PyValueError::new_err("min_f64 requires a value"))?
                .extract()?,
        ),
        "max_f64" => Constraint::MaxF64(
            v.ok_or_else(|| PyValueError::new_err("max_f64 requires a value"))?
                .extract()?,
        ),
        "min_length" => Constraint::MinLength(
            v.ok_or_else(|| PyValueError::new_err("min_length requires a value"))?
                .extract()?,
        ),
        "max_length" => Constraint::MaxLength(
            v.ok_or_else(|| PyValueError::new_err("max_length requires a value"))?
                .extract()?,
        ),
        "regex" => Constraint::Regex(
            v.ok_or_else(|| PyValueError::new_err("regex requires a value"))?
                .extract()?,
        ),
        "email" => Constraint::Email,
        "url" => Constraint::Url,
        "nonempty" | "non_empty" => Constraint::NonEmpty,
        _ => {
            return Err(PyValueError::new_err(format!(
                "unknown constraint kind {k:?}"
            )))
        }
    };
    Ok(vec![one])
}

fn fields_from_model(
    py: Python<'_>,
    cls: &Bound<'_, PyAny>,
    depth: usize,
) -> PyResult<Vec<FieldDef>> {
    let hints = get_type_hints(py, cls)?;
    let hints = hints.bind(py);
    let mut out = Vec::with_capacity(hints.len());
    for (k, v) in hints.iter() {
        let name: String = k.extract()?;
        let constraints = field_constraints_from_annotated(py, &v)?;
        let ty = py_to_typra_type(py, &v, depth + 1)?;
        let path = FieldPath::new([Cow::Owned(name.clone())])
            .map_err(|e| PyValueError::new_err(format!("invalid field name {name:?}: {e}")))?;
        out.push(FieldDef {
            path,
            ty,
            constraints,
        });
    }
    Ok(out)
}

fn indexes_from_model(
    _py: Python<'_>,
    cls: &Bound<'_, PyAny>,
) -> PyResult<Vec<typra_core::schema::IndexDef>> {
    use typra_core::schema::{IndexDef, IndexKind};
    let mut out = Vec::new();
    let Ok(v) = cls.getattr("__typra_indexes__") else {
        return Ok(out);
    };
    if v.is_none() {
        return Ok(out);
    }
    let list = v.cast::<PyList>().map_err(|_| {
        PyValueError::new_err("__typra_indexes__ must be a list of typra.models.index/unique specs")
    })?;
    for item in list.iter() {
        let spec: IndexSpec = item.extract().map_err(|_| {
            PyValueError::new_err(
                "__typra_indexes__ must contain IndexSpec values from typra.models.index/unique",
            )
        })?;
        let name = spec
            .name
            .clone()
            .unwrap_or_else(|| format!("{}_idx", spec.field));
        let kind = if spec.unique {
            IndexKind::Unique
        } else {
            IndexKind::NonUnique
        };
        let path = FieldPath::new([Cow::Owned(spec.field.clone())]).map_err(|e| {
            PyValueError::new_err(format!("invalid index field {}: {e}", spec.field))
        })?;
        out.push(IndexDef { name, path, kind });
    }
    Ok(out)
}

fn normalize_value(py: Python<'_>, v: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
    // enum.Enum -> use `.value` if it's str/int/float/bool, else `.name`
    if let Ok(enum_mod) = PyModule::import(py, "enum") {
        if let Ok(enum_base) = enum_mod.getattr("Enum") {
            if v.is_instance(&enum_base)? {
                let value = v.getattr("value")?;
                if value.cast::<PyString>().is_ok()
                    || value.extract::<i64>().is_ok()
                    || value.extract::<f64>().is_ok()
                    || value.extract::<bool>().is_ok()
                {
                    return Ok(value.unbind());
                }
                let name = v.getattr("name")?;
                return Ok(name.unbind());
            }
        }
    }
    Ok(v.clone().unbind())
}

fn obj_to_row_dict(
    py: Python<'_>,
    obj: &Bound<'_, PyAny>,
    is_pydantic: bool,
) -> PyResult<Py<PyDict>> {
    if is_pydantic {
        let d = obj.call_method0("model_dump")?;
        let d = d.cast_into::<PyDict>()?;
        let out = PyDict::new(py);
        for (k, v) in d.iter() {
            out.set_item(k, normalize_value(py, &v)?)?;
        }
        return Ok(out.unbind());
    }
    let dc = dataclasses_mod(py)?;
    let asdict = dc.getattr("asdict")?;
    let d = asdict.call1((obj,))?.cast_into::<PyDict>()?;
    let out = PyDict::new(py);
    for (k, v) in d.iter() {
        out.set_item(k, normalize_value(py, &v)?)?;
    }
    Ok(out.unbind())
}

fn dict_to_obj(
    _py: Python<'_>,
    cls: &Bound<'_, PyAny>,
    d: &Bound<'_, PyDict>,
    is_pydantic: bool,
) -> PyResult<Py<PyAny>> {
    if is_pydantic {
        let v = cls.call_method1("model_validate", (d,))?;
        return Ok(v.unbind());
    }
    let kwargs = d;
    let v = cls.call((), Some(kwargs))?;
    Ok(v.unbind())
}

/// A registered model-backed collection.
#[pyclass]
pub struct ModelCollection {
    db: Py<Database>,
    name: String,
    model_cls: Py<PyAny>,
    is_pydantic: bool,
}

/// Query builder that returns model instances.
#[pyclass]
pub struct ModelQuery {
    inner: Py<PyAny>,
    model_cls: Py<PyAny>,
    is_pydantic: bool,
    selected_fields: Option<Py<PyAny>>,
}

#[pyfunction]
pub fn collection(
    py: Python<'_>,
    db: Py<Database>,
    model_cls: Bound<'_, PyAny>,
) -> PyResult<ModelCollection> {
    let name = collection_name_for(py, &model_cls)?;
    let pk = primary_key_for(&model_cls)?;

    let is_pyd = pydantic_is_model(py, &model_cls)?;
    let is_dc = is_dataclass(py, &model_cls)?;
    if !is_pyd && !is_dc {
        return Err(PyValueError::new_err(
            "model must be a dataclass or a pydantic.BaseModel subclass",
        ));
    }

    // Inject field refs so `Book.title` is usable as a path object.
    ensure_field_refs(py, &model_cls)?;

    // Build fields and indexes.
    let fields = fields_from_model(py, &model_cls, 0)?;
    let indexes = indexes_from_model(py, &model_cls)?;

    // Register collection if missing; otherwise leave as-is.
    let db_ref = db.bind(py).borrow();
    let exists = {
        let g = lock_inner(&db_ref.inner)?;
        g.collection_id_named(&name).is_ok()
    };
    if !exists {
        let mut g = lock_inner(&db_ref.inner)?;
        let _ = g
            .register_collection_with_indexes(&name, fields, indexes, &pk)
            .map_err(db_error_to_py)?;
    }

    Ok(ModelCollection {
        db,
        name,
        model_cls: model_cls.unbind(),
        is_pydantic: is_pyd,
    })
}

fn path_any_to_py<'py>(py: Python<'py>, path: &Bound<'py, PyAny>) -> PyResult<Bound<'py, PyAny>> {
    if let Ok(fr) = path.extract::<PyRef<'_, FieldRef>>() {
        let tup = PyTuple::new(py, fr.path.iter().map(|s| s.as_str()))?;
        return Ok(tup.into_any());
    }
    Ok(path.clone())
}

#[pymethods]
impl ModelCollection {
    #[getter]
    fn name(&self) -> String {
        self.name.clone()
    }

    fn insert(&self, py: Python<'_>, obj: &Bound<'_, PyAny>) -> PyResult<()> {
        let d = obj_to_row_dict(py, obj, self.is_pydantic)?;
        let db = self.db.bind(py);
        db.call_method1("insert", (&self.name, d.bind(py)))?;
        Ok(())
    }

    fn get(&self, py: Python<'_>, pk: &Bound<'_, PyAny>) -> PyResult<Option<Py<PyAny>>> {
        let db = self.db.bind(py);
        let row = db.call_method1("get", (&self.name, pk))?;
        if row.is_none() {
            return Ok(None);
        }
        let d = row.cast::<PyDict>()?;
        let cls = self.model_cls.bind(py);
        let obj = dict_to_obj(py, cls, d, self.is_pydantic)?;
        Ok(Some(obj))
    }

    #[pyo3(name = "where")]
    fn where_(
        &self,
        py: Python<'_>,
        path: &Bound<'_, PyAny>,
        value: &Bound<'_, PyAny>,
    ) -> PyResult<ModelQuery> {
        // Delegate to the existing Python query builder: db.collection(name).where(...)
        let db = self.db.bind(py);
        let col = db.call_method1("collection", (&self.name,))?;
        let path = path_any_to_py(py, path)?;
        let qb = col.call_method1("where", (path, value))?;
        Ok(ModelQuery {
            inner: qb.unbind(),
            model_cls: self.model_cls.clone_ref(py),
            is_pydantic: self.is_pydantic,
            selected_fields: None,
        })
    }

    #[pyo3(signature = (*, fields=None))]
    fn all(&self, py: Python<'_>, fields: Option<&Bound<'_, PyAny>>) -> PyResult<Vec<Py<PyAny>>> {
        let db = self.db.bind(py);
        let col = db.call_method1("collection", (&self.name,))?;
        let rows_any = match fields {
            None => col.call_method0("all")?,
            Some(f) => {
                let kwargs = PyDict::new(py);
                kwargs.set_item("fields", f)?;
                col.call_method("all", (), Some(&kwargs))?
            }
        };
        let rows = rows_any.cast::<PyList>()?;
        let cls = self.model_cls.bind(py);
        let mut out = Vec::with_capacity(rows.len());
        for item in rows.iter() {
            let d = item.cast::<PyDict>()?;
            out.push(dict_to_obj(py, cls, d, self.is_pydantic)?);
        }
        Ok(out)
    }

    fn update(
        &self,
        py: Python<'_>,
        pk: &Bound<'_, PyAny>,
        patch: &Bound<'_, PyAny>,
    ) -> PyResult<()> {
        let db = self.db.bind(py);
        let current = db.call_method1("get", (&self.name, pk))?;
        if current.is_none() {
            return Err(PyValueError::new_err("cannot update missing row"));
        }
        let current = current.cast::<PyDict>()?;

        let patch_dict = if let Ok(d) = patch.cast::<PyDict>() {
            d.clone().unbind()
        } else {
            obj_to_row_dict(py, patch, self.is_pydantic)?
        };
        let patch_dict = patch_dict.bind(py);

        let merged = PyDict::new(py);
        for (k, v) in current.iter() {
            merged.set_item(k, v)?;
        }
        for (k, v) in patch_dict.iter() {
            merged.set_item(k, v)?;
        }
        db.call_method1("insert", (&self.name, merged))?;
        Ok(())
    }
}

#[pymethods]
impl ModelQuery {
    fn select(&self, py: Python<'_>, fields: &Bound<'_, PyAny>) -> PyResult<Self> {
        Ok(Self {
            inner: self.inner.clone_ref(py),
            model_cls: self.model_cls.clone_ref(py),
            is_pydantic: self.is_pydantic,
            selected_fields: Some(fields.clone().unbind()),
        })
    }

    fn and_where(
        &self,
        py: Python<'_>,
        path: &Bound<'_, PyAny>,
        value: &Bound<'_, PyAny>,
    ) -> PyResult<Self> {
        let path = path_any_to_py(py, path)?;
        let qb = self
            .inner
            .bind(py)
            .call_method1("and_where", (path, value))?;
        Ok(Self {
            inner: qb.unbind(),
            model_cls: self.model_cls.clone_ref(py),
            is_pydantic: self.is_pydantic,
            selected_fields: opt_pyany_clone_ref(py, &self.selected_fields),
        })
    }

    fn limit(&self, py: Python<'_>, n: usize) -> PyResult<Self> {
        let qb = self.inner.bind(py).call_method1("limit", (n,))?;
        Ok(Self {
            inner: qb.unbind(),
            model_cls: self.model_cls.clone_ref(py),
            is_pydantic: self.is_pydantic,
            selected_fields: opt_pyany_clone_ref(py, &self.selected_fields),
        })
    }

    fn explain(&self, py: Python<'_>) -> PyResult<String> {
        self.inner.bind(py).call_method0("explain")?.extract()
    }

    #[pyo3(signature = (*, fields=None))]
    fn all(&self, py: Python<'_>, fields: Option<&Bound<'_, PyAny>>) -> PyResult<Vec<Py<PyAny>>> {
        let fields = match (fields, self.selected_fields.as_ref()) {
            (Some(f), _) => Some(f.clone()),
            (None, Some(sel)) => Some(sel.bind(py).clone()),
            (None, None) => None,
        };
        let rows_any = match fields.as_ref() {
            None => self.inner.bind(py).call_method0("all")?,
            Some(f) => {
                let kwargs = PyDict::new(py);
                kwargs.set_item("fields", f)?;
                self.inner.bind(py).call_method("all", (), Some(&kwargs))?
            }
        };
        let rows = rows_any.cast::<PyList>()?;
        let cls = self.model_cls.bind(py);
        let mut out = Vec::with_capacity(rows.len());
        for item in rows.iter() {
            let d = item.cast::<PyDict>()?;
            out.push(dict_to_obj(py, cls, d, self.is_pydantic)?);
        }
        Ok(out)
    }
}

fn opt_pyany_clone_ref(py: Python<'_>, v: &Option<Py<PyAny>>) -> Option<Py<PyAny>> {
    v.as_ref().map(|x| x.clone_ref(py))
}

#[pyfunction]
pub fn plan(py: Python<'_>, db: Py<Database>, model_cls: Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
    let name = collection_name_for(py, &model_cls)?;
    let fields = fields_from_model(py, &model_cls, 0)?;
    let indexes = indexes_from_model(py, &model_cls)?;
    let fields_json = schema_to_fields_json(py, &fields)?;
    let indexes_json = schema_to_indexes_json(py, &indexes)?;
    let db = db.bind(py);
    Ok(db
        .call_method1("plan_schema_version", (&name, fields_json, indexes_json))?
        .extract::<Py<PyAny>>()?)
}

#[pyfunction]
#[pyo3(signature = (db, model_cls, *, force=false))]
pub fn apply(
    py: Python<'_>,
    db: Py<Database>,
    model_cls: Bound<'_, PyAny>,
    force: bool,
) -> PyResult<u32> {
    let name = collection_name_for(py, &model_cls)?;
    let fields = fields_from_model(py, &model_cls, 0)?;
    let indexes = indexes_from_model(py, &model_cls)?;
    let fields_json = schema_to_fields_json(py, &fields)?;
    let indexes_json = schema_to_indexes_json(py, &indexes)?;
    let db = db.bind(py);
    db.call_method(
        "register_schema_version",
        (&name, fields_json, indexes_json),
        Some(&{
            let kwargs = PyDict::new(py);
            kwargs.set_item("force", force)?;
            kwargs
        }),
    )?
    .extract()
}

fn schema_to_fields_json(_py: Python<'_>, fields: &[FieldDef]) -> PyResult<String> {
    use serde_json::json;
    use typra_core::schema::Constraint;
    fn ty_to_json(ty: &Type) -> serde_json::Value {
        match ty {
            Type::Bool => json!("bool"),
            Type::Int64 => json!("int64"),
            Type::Uint64 => json!("uint64"),
            Type::Float64 => json!("float64"),
            Type::String => json!("string"),
            Type::Bytes => json!("bytes"),
            Type::Uuid => json!("uuid"),
            Type::Timestamp => json!("timestamp"),
            Type::Optional(inner) => json!({"optional": ty_to_json(inner)}),
            Type::List(inner) => json!({"list": ty_to_json(inner)}),
            Type::Object(fields) => {
                let fs: Vec<_> = fields.iter().map(field_to_json).collect();
                json!({"object": fs})
            }
            Type::Enum(vars) => json!({"enum": vars}),
        }
    }
    fn constraint_to_json(c: &Constraint) -> serde_json::Value {
        match c {
            Constraint::MinI64(n) => json!({"min_i64": n}),
            Constraint::MaxI64(n) => json!({"max_i64": n}),
            Constraint::MinU64(n) => json!({"min_u64": n}),
            Constraint::MaxU64(n) => json!({"max_u64": n}),
            Constraint::MinF64(n) => json!({"min_f64": n}),
            Constraint::MaxF64(n) => json!({"max_f64": n}),
            Constraint::MinLength(n) => json!({"min_length": n}),
            Constraint::MaxLength(n) => json!({"max_length": n}),
            Constraint::Regex(s) => json!({"regex": s}),
            Constraint::Email => json!({"email": true}),
            Constraint::Url => json!({"url": true}),
            Constraint::NonEmpty => json!({"nonempty": true}),
        }
    }
    fn field_to_json(f: &FieldDef) -> serde_json::Value {
        let path: Vec<_> = f.path.0.iter().map(|s| s.as_ref()).collect();
        let mut o = json!({
            "path": path,
            "type": ty_to_json(&f.ty),
        });
        if !f.constraints.is_empty() {
            if let Some(obj) = o.as_object_mut() {
                obj.insert(
                    "constraints".to_string(),
                    serde_json::Value::Array(
                        f.constraints.iter().map(constraint_to_json).collect(),
                    ),
                );
            }
        }
        o
    }
    let arr: Vec<_> = fields.iter().map(field_to_json).collect();
    serde_json::to_string(&arr).map_err(|e| PyValueError::new_err(e.to_string()))
}

fn schema_to_indexes_json(
    _py: Python<'_>,
    indexes: &[typra_core::schema::IndexDef],
) -> PyResult<String> {
    use serde_json::json;
    let arr: Vec<_> = indexes
        .iter()
        .map(|idx| {
            let path: Vec<_> = idx.path.0.iter().map(|s| s.as_ref()).collect();
            let kind = match idx.kind {
                typra_core::schema::IndexKind::Unique => "unique",
                typra_core::schema::IndexKind::NonUnique => "index",
            };
            json!({"name": idx.name, "path": path, "kind": kind})
        })
        .collect();
    serde_json::to_string(&arr).map_err(|e| PyValueError::new_err(e.to_string()))
}
