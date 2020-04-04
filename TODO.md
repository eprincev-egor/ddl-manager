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
- serial => integer (do not migrate)
+ field: check for check constraints (need for migrate another condition)
    information_schema.check_constraints.check_clause