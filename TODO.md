+ cache name should be unique for table
+ cancel update rows from prev run (Migrator)
+ new command: update cache rows
+ fix long names > 64
    + fix signature (for detect changes)
    + fix object.toSQL 
    + log warning: name XXXX too long (> 64 symbols)
- test errors on:
    + hard select query
      + sub query
      + group by
      + with
      + union
    + no column name in select
    - no columns
    + column name is not unique
    + exists same column name in another cache in another file
    - exists column in db with another type
+ diff: compare caches
- Migrator: 
  + i can rename cache but, don't need recreate columns and update rows,
        need change only triggers
  + do not drop cache columns if no changes
  + do not drop cache columns if same columns exists in db
  + need REAL load schema
  - universal agg
  - first agg
  - last agg
  - date_or_null_agg
  + cache like are
    cache totals for companies (
      select companies.id * 2
    )
      ! before insert/update => need listen more fields in other caches
      => use after
+ other helper functions
+ don't create cache triggers for some tables
    cache totals for companies (
      select
        string_agg(distinct order_type.name) as orders_types_names
      from orders
      left join order_type on
        order_type.id = orders.id_order_type
    )
    without triggers on order_type

- fix renaming file or directory
- find and fix any TODO:
+ log building cache
- log update cache columns
- when need drop function while trigger is frozen, but function not
- timeline