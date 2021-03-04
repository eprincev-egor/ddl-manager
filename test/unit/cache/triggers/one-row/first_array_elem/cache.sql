cache gtd for comments (
    select
        gtd.orders_ids[1] * 3 as gtd_order_id

    from list_gtd as gtd
    where
        gtd.id = comments.row_id and
        gtd.deleted = 0 and

        -- У заказа с ДТ связь M:M, поэтому, если заказов больше 1,
        -- возвращаем null, так как не знаем, какой выбрать
        array_length_excluding_nulls( gtd.orders_ids ) = 1 and

        comments.query_name in (
            'LIST_ALL_GTD',
            'LIST_ARCHIVE_GTD',
            'LIST_ACTIVE_GTD',
            'LIST_GTD'
        )
)
-- without insert case on list_gtd