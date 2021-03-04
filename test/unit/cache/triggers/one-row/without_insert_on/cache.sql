cache gtd_without_insert for comments (
    select
        gtd.orders_ids as gtd_orders_ids

    from list_gtd as gtd
    where
        gtd.id = comments.row_id and
        gtd.deleted = 0 and

        comments.query_name in (
            'LIST_ALL_GTD',
            'LIST_ARCHIVE_GTD',
            'LIST_ACTIVE_GTD',
            'LIST_GTD'
        )
)
without insert case on list_gtd