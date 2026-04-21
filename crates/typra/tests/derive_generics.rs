use typra::DbModel;

#[derive(DbModel)]
struct WithLifetime<'a> {
    _s: std::marker::PhantomData<&'a ()>,
}

#[derive(DbModel)]
struct WithTypeParam<T> {
    _t: std::marker::PhantomData<T>,
}

fn assert_model<T: DbModel>() {}

#[test]
fn derive_supports_lifetime_and_type_params() {
    assert_model::<WithLifetime<'static>>();
    assert_model::<WithTypeParam<u32>>();
}

