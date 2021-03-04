cache managers_and_watchers for tasks (
    select
        tasks.watchers_ids ||
        tasks.orders_managers_ids as watchers_or_managers
)