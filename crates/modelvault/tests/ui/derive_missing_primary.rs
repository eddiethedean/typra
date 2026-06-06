use modelvault::DbModel;

#[derive(DbModel)]
struct MissingPrimary {
    id: i64,
}
