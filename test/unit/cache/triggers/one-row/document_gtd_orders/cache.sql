cache gtd for list_documents (
    select
        gtd.orders_ids as gtd_orders_ids

    from list_gtd as gtd
    where
        gtd.id = list_documents.table_id and
        gtd.deleted = 0 and
        list_documents.table_name in (
            'LIST_GTD_ACTIVE',
            'LIST_GTD_ARCHIVE'
        )
)
index gin on (gtd_orders_ids)