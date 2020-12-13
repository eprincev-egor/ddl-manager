create or replace function cache_totals_for_some_report_row_on_orders()
returns trigger as $body$
begin

    if TG_OP = 'DELETE' then

        update some_report_row set
            orders_total = orders_total - coalesce(old.profit, 0);

        return old;
    end if;

    if TG_OP = 'UPDATE' then
        if new.profit is not distinct from old.profit then
            return new;
        end if;



        update some_report_row set
            orders_total = orders_total - coalesce(old.profit, 0);

        update some_report_row set
            orders_total = orders_total + coalesce(new.profit, 0);

        return new;
    end if;

    if TG_OP = 'INSERT' then

        update some_report_row set
            orders_total = orders_total + coalesce(new.profit, 0);

        return new;
    end if;

end
$body$
language plpgsql;

create trigger cache_totals_for_some_report_row_on_orders
after insert or update of profit or delete
on public.orders
for each row
execute procedure cache_totals_for_some_report_row_on_orders();