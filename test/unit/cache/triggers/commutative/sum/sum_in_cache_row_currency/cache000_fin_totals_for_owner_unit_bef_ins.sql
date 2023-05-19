create or replace function cache000_fin_totals_for_owner_unit_bef_ins()
returns trigger as $body$
declare new_totals record;
begin



    select
        sum(
            round(
                coalesce(
                    fin_operation.sum_vat,
                    0 :: bigint
                ) :: numeric * get_curs(
                    fin_operation.id_currency,
                    new.id_currency_fin_oper,
                    fin_operation.curs,
                    coalesce(
                        fin_operation.doc_date,
                        now_utc()
                    ),
                    fin_operation.is_euro_zone_curs
                ) :: numeric,
                - 2
            ) :: bigint / array_length(
                fin_operation.units_ids,
                1
            )
        ) as fin_sum
    from fin_operation
    where
        fin_operation.units_ids && new.units_ids
        and
        fin_operation.deleted = 0
    into new_totals;


    new.fin_sum = new_totals.fin_sum;


    return new;
end
$body$
language plpgsql;

create trigger cache000_fin_totals_for_owner_unit_bef_ins
before insert
on operation.owner_unit
for each row
execute procedure cache000_fin_totals_for_owner_unit_bef_ins();