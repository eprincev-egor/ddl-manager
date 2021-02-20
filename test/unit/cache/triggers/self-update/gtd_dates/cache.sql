cache self_dates for list_gtd (
    select
        coalesce(
            list_gtd.date_clear,
            list_gtd.date_conditional_clear,
            list_gtd.date_release_for_procuring
        ) as clear_date_total
)