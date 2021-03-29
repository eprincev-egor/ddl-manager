cache totals for cache_table (
    select
        sum(trigger_table.profit) as total_profit
    
    from trigger_table
    where
        cache_table.id = any( trigger_table.unknown_ids )
)