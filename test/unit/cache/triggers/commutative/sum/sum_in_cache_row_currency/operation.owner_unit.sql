create or replace function cache_fin_totals_for_self_on_owner_unit()
returns trigger as $body$
begin
    if TG_OP = 'INSERT' then
        if new.units_ids is null then
            return new;
        end if;
    end if;

    if TG_OP = 'UPDATE' then
        if
            new.id_currency_fin_oper is not distinct from old.id_currency_fin_oper
            and
            new.units_ids is not distinct from old.units_ids
        then
            return new;
        end if;
    end if;


    update operation.owner_unit as own_unit set
        (
            fin_sum
        ) = (
            select
                sum(
        round(
            coalesce(
                fin_operation.sum_vat,
                0 :: bigint
            ) :: numeric * get_curs(
                fin_operation.id_currency,
                own_unit.id_currency_fin_oper,
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
                fin_operation.units_ids && own_unit.units_ids
    and
    fin_operation.deleted = 0
        )
    where
        own_unit.id = new.id;


    return new;
end
$body$
language plpgsql;

create trigger cache_fin_totals_for_self_on_owner_unit
after insert or update of id_currency_fin_oper, units_ids
on operation.owner_unit
for each row
execute procedure cache_fin_totals_for_self_on_owner_unit();