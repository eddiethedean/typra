use modelvault::DbModel;

#[derive(DbModel)]
struct DuplicatePrimary {
    #[db(primary)]
    a: i64,
    #[db(primary)]
    b: String,
}
