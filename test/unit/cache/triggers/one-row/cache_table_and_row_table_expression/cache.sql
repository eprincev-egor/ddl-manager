cache user_task for comments (
    select
        coalesce(
            user_task.query_name,
            comments.query_name
        ) as target_query_name,

        coalesce(
            user_task.row_id,
            comments.row_id
        ) as target_row_id

    from user_task
    where
        user_task.id = comments.row_id and
        user_task.deleted = 0 and
        comments.query_name = 'USER_TASK'
)
without insert case on user_task