create or replace function cache_totals_for_orders_on_invoice_positions()
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
            select old_row.cost, old_row.invoice_id
            union
            select new_row.cost, new_row.invoice_id
        )
    update orders set
        (
            invoice_positions_cost
        ) = (
            select
                coalesce(
        sum(invoice_positions.cost),
        0
    )
    :: numeric as invoice_positions_cost

            from invoice_positions

            inner join invoices on
                invoices.id = invoice_positions.invoice_id

            where
                invoices.id_order = orders.id
        )
    from changed_rows, invoices
    where
        invoices.id_order = orders.id
        and
        invoices.id = changed_rows.invoice_id;

    return return_row;
end
$body$
language plpgsql;

create trigger cache_totals_for_orders_on_invoice_positions
after insert or update of cost, invoice_id or delete
on public.invoice_positions
for each row
execute procedure cache_totals_for_orders_on_invoice_positions();