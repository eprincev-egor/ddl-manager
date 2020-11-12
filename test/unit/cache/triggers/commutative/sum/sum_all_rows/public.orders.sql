create or replace function cache_totals_for_some_report_row_on_orders()
returns trigger as $body$
begin

    if TG_OP = 'DELETE' then

        if coalesce(old.profit, 0) != 0 then
            update some_report_row set
                orders_total = orders_total - coalesce(old.profit, 0);
        end if;

        return old;
    end if;

    if TG_OP = 'UPDATE' then
        if new.profit is not distinct from old.profit then
            return new;
        end if;



        if coalesce(old.profit, 0) != 0 then
            update some_report_row set
                orders_total = orders_total - coalesce(old.profit, 0);
        end if;

        if coalesce(new.profit, 0) != 0 then
            update some_report_row set
                orders_total = orders_total + coalesce(new.profit, 0);
        end if;

        return new;
    end if;

    if TG_OP = 'INSERT' then

        if coalesce(new.profit, 0) != 0 then
            update some_report_row set
                orders_total = orders_total + coalesce(new.profit, 0);
        end if;

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