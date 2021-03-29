cache totals for cache_table (
    select
        sum(trigger_table.profit) as total_profit
    
    from trigger_table
    where
        ARRAY[ cache_table.id ]::bigint[] <@ trigger_table.unknown_ids
)