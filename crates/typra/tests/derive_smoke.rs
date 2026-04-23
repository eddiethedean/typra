use typra::DbModel;

#[derive(DbModel)]
#[allow(dead_code)]
struct Smoke {
    #[db(primary)]
    id: String,
}

fn assert_model<T: DbModel>() {}

#[test]
fn derive_implies_db_model() {
    assert_model::<Smoke>();
}
