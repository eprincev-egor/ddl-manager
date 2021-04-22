create or replace function cache_border_crossing_for_self_on_order()
returns trigger as $body$
begin

    if TG_OP = 'UPDATE' then
        if new.date_delivery is not distinct from old.date_delivery then
            return new;
        end if;
    end if;


    update public.order as orders set
        (
            id_border_crossing,
            date_delivery
        ) = (
            select
                border_crossing.id as id_border_crossing,
                coalesce(
        border_crossing.end_expected_date,
        orders.date_delivery
    ) as date_delivery

            from operation.operation as border_crossing

            where
                border_crossing.id_order = orders.id
    and
    border_crossing.is_border_crossing = 1
    and
    border_crossing.id_doc_parent_operation is null
    and
    border_crossing.deleted = 0
order by
    border_crossing.id desc nulls first
limit 1
        )
    where
        orders.id = new.id;


    return new;
end
$body$
language plpgsql;

create trigger cache_border_crossing_for_self_on_order
after insert or update of date_delivery
on public.order
for each row
execute procedure cache_border_crossing_for_self_on_order();