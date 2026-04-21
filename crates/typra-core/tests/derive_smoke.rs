use typra_core::prelude::*;
use typra_derive::DbModel;

#[derive(DbModel)]
struct Smoke;

fn assert_model<T: DbModel>() {}

#[test]
fn derive_implies_db_model() {
    assert_model::<Smoke>();
}
