create or replace function cache_orders_for_invoice_on_unit()
returns trigger as $body$
declare new_row record;
declare old_row record;
declare return_row record;
begin
    if TG_OP = 'DELETE' then
        return_row = old;
    else
        return_row = new;
    end if;

    new_row = return_row;
    old_row = return_row;

    if TG_OP in ('INSERT', 'UPDATE') then
        new_row = new;
    end if;
    if TG_OP in ('UPDATE', 'DELETE') then
        old_row = old;
    end if;

    with
        changed_rows as (
            select old_row.id, old_row.id_order
            union
            select new_row.id, new_row.id_order
        )
    update invoice set
        (
            orders_profit
        ) = (
            select
                coalesce(sum(orders.profit), 0) as orders_profit

            from invoice_positions as inv_pos

            left join operation.unit on
                operation.unit.id = inv_pos.id_unit

            left join orders on
                orders.id = coalesce(
    operation.unit.id_order,
    inv_pos.id_unit
)

            where
                inv_pos.id_invoice = invoice.id
    and
    inv_pos.deleted = 0
        )
    from changed_rows, invoice_positions as inv_pos
    where
        inv_pos.id_invoice = invoice.id
        and
        inv_pos.deleted = 0
        and
        changed_rows.id = inv_pos.id_unit;

    return return_row;
end
$body$
language plpgsql;

create trigger cache_orders_for_invoice_on_unit
after insert or update of id_order or delete
on operation.unit
for each row
execute procedure cache_orders_for_invoice_on_unit();