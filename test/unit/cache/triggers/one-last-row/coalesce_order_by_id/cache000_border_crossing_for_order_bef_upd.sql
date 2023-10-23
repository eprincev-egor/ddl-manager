create or replace function cache000_border_crossing_for_order_bef_upd()
returns trigger as $body$
declare new_totals record;
begin

    if new.date_delivery is not distinct from old.date_delivery then
        return new;
    end if;


    select
            border_crossing.id as __border_crossing_id,
            coalesce(
                border_crossing.end_expected_date,
                new.date_delivery
                        ) as date_delivery,
            border_crossing.id as id_border_crossing
    from operation.operation as border_crossing
    where
        border_crossing.id_order = new.id
        and
        border_crossing.is_border_crossing = 1
        and
        border_crossing.id_doc_parent_operation is null
        and
        border_crossing.deleted = 0
    order by
        border_crossing.id desc nulls first
    limit 1
    into new_totals;


    new.__border_crossing_id = new_totals.__border_crossing_id;
    new.date_delivery = new_totals.date_delivery;
    new.id_border_crossing = new_totals.id_border_crossing;


    return new;
end
$body$
language plpgsql;

create trigger cache000_border_crossing_for_order_bef_upd
before update of date_delivery
on public.order
for each row
execute procedure cache000_border_crossing_for_order_bef_upd();