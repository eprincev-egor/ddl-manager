- cache name should be unique for table
- cancel update rows from prev run (Migrator)
+ fix long names > 64
    + fix signature (for detect changes)
    + fix object.toSQL 
    + log warning: name XXXX too long (> 64 symbols)
- test errors on:
    - hard select query
    - no column name in select
    - column name is not unique
    - exists same column name in another cache in another file
+ diff: compare caches
- Migrator: 
  + i can rename cache but, don't need recreate columns and update rows,
        need change only triggers
  + do not drop cache columns if no changes
  - do not drop cache columns if same columns exists in db
- Migrator: need REAL load schema
- fix renaming file or directory