create or replace function cache000_fin_totals_for_owner_unit_bef_upd()
returns trigger as $body$
declare new_totals record;
begin

    if
        new.id_currency_fin_oper is not distinct from old.id_currency_fin_oper
        and
        new.units_ids is not distinct from old.units_ids
    then
        return new;
    end if;


    select
            ('{' || string_agg(
                                        '"' || public.fin_operation.id::text || '":' || jsonb_build_object(
                            'curs', public.fin_operation.curs,'deleted', public.fin_operation.deleted,'doc_date', public.fin_operation.doc_date,'id', public.fin_operation.id,'id_currency', public.fin_operation.id_currency,'is_euro_zone_curs', public.fin_operation.is_euro_zone_curs,'sum_vat', public.fin_operation.sum_vat,'units_ids', public.fin_operation.units_ids
                        )::text,
                                        ','
                                    ) || '}')
            ::
            jsonb as __fin_totals_json__,
            sum(
                round(
                    coalesce(
                        fin_operation.sum_vat,
                        0::bigint
                        )::numeric *     get_curs(
                        fin_operation.id_currency,
                        new.id_currency_fin_oper,
                        fin_operation.curs,
                        coalesce(
                            fin_operation.doc_date,
                            now_utc()
                            ),
                        fin_operation.is_euro_zone_curs
                        )::numeric,
                    - 2
                    )::bigint /     array_length(
                    fin_operation.units_ids,
                    1
                    )
                        ) as fin_sum
    from fin_operation
    where
        fin_operation.units_ids && cm_build_array_for((
                        select units_ids
                        from public.fin_operation
                        where false
                    ), new.units_ids)
        and
        fin_operation.deleted = 0
    into new_totals;


    new.__fin_totals_json__ = new_totals.__fin_totals_json__;
    new.fin_sum = new_totals.fin_sum;


    return new;
end
$body$
language plpgsql;

create trigger cache000_fin_totals_for_owner_unit_bef_upd
before update of id_currency_fin_oper, units_ids
on operation.owner_unit
for each row
execute procedure cache000_fin_totals_for_owner_unit_bef_upd();