cache watchers for user_task (
    select 
        array_agg(DISTINCT link.id_user ) as watchers_users_ids
    from user_task_watcher_link as link
    where
        link.id_user_task = user_task.id
)