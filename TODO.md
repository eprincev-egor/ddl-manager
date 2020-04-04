- check migrations before build
- change schema for table
- indexes
+ extensions
- test order of migrations (1.tables 2.columns 3.functions 4.triggers 5.views 6.foreignKeys)
- fs: delete dir
- fs: create dir
- fs: rename dir
- dump
- watch
- logs (renamed dir, create file, removed file, ...)
- build
- watch
- tests
- timeline
- table inherits
- smallserial, serial, bigserial => smallint, integer, bigint (do not migrate)
    - columnModel: add default
    - test migration with columns default
    - test dbDriver with columns default
    - test parser with columns default
    - test fs with columns default
+ field: check for check constraints (need for migrate another condition)
    information_schema.check_constraints.check_clause
- triggerModel: add events, 'when condition'
- foreignKeyModel: add other parameters