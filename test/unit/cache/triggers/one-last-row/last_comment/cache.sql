cache last_comment for operation.unit (
    select
        comments.message as last_comment
    from comments
    where
        comments.unit_id = operation.unit.id

    order by comments.id desc
    limit 1
)