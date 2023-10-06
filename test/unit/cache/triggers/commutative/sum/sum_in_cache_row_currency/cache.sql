cache fin_totals for operation.owner_unit as own_unit (
    select
        sum(
            round(
                coalesce(
                    fin_operation.sum_vat,
                    0::bigint
                )::numeric * get_curs(
                    fin_operation.id_currency,
                    own_unit.id_currency_fin_oper,
                    fin_operation.curs,
                    coalesce(
                        fin_operation.doc_date,
                        now_utc()
                    ),
                    fin_operation.is_euro_zone_curs
                )
               ::numeric,
                - 2
            )::bigint

            / array_length( fin_operation.units_ids, 1 )
        ) as fin_sum

    from fin_operation
    where
        fin_operation.units_ids && own_unit.units_ids and
        fin_operation.deleted = 0
)